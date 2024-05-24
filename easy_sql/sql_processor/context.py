from __future__ import annotations

import os
import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from easy_sql.utils.sql_expr import CommentSubstitutor, comment_start

from ..logger import logger

if TYPE_CHECKING:
    from . import FuncRunner

from .common import Column, SqlProcessorException, VarsReplacer

__all__ = ["VarsContext", "TemplatesContext", "ProcessorContext"]


class VarsContext(VarsReplacer):
    def __init__(
        self,
        vars: Optional[Dict[str, Any]] = None,
        list_vars: Optional[Dict[str, List]] = None,
        debug_log: bool = False,
    ):
        self.vars = {}
        self.list_vars = list_vars or {}
        self.func_runner = None
        self.debug_log = debug_log
        self.add_vars(vars or {})

    def init(self, func_runner: FuncRunner):
        self.func_runner = func_runner

    def _get_var_value(self, var_name: str, original_text: str) -> Any:
        var_name, default_value = (
            (var_name[: var_name.index(":")], var_name[var_name.index(":") + 1 :])
            if ":" in var_name
            else (var_name, None)
        )
        variables = self.vars
        if var_name.lower() in variables:
            return variables[var_name.lower()]
        elif default_value is not None:
            return default_value
        else:
            raise SqlProcessorException(f"unknown variable `{var_name}`. text={original_text}, known_vars={variables}")

    def replace_variables(self, text: str, include_funcs: bool = True) -> str:
        return self._replace_variables(text, include_funcs=include_funcs)

    def _replace_variables(self, text: str, include_funcs: bool = True, comment_substituted: bool = False) -> str:
        original_text = text

        m = re.match(r"^\${([^}]+)}$", text.strip())
        if m:
            var_name = m.group(1).strip()
            default_value = None
            if ":" in var_name:
                var_name = var_name[: var_name.index(":")].strip()
                default_value = var_name[var_name.index(":") + 1 :].strip()
            result = self.vars.get(var_name, self.vars.get(var_name.lower(), default_value))
            if isinstance(result, str) and "${" in result:
                return self._replace_variables(
                    result, include_funcs=include_funcs, comment_substituted=comment_substituted
                )
            else:
                return result

        if not comment_substituted:
            comment_substitutor = CommentSubstitutor()
            text = comment_substitutor.substitute(text)

        if include_funcs:
            # skip funcs first, and then handle funcs to support to vars in func call, e.g: ${f(${a}, xx${${b}})}
            # fix comment_substituted to True, since text is either comment_substituted already
            #   or be comment_substituted at an earlier step
            text = self._replace_variables(text, False, comment_substituted=True)
            self._log_replace_process(f"after vars replaced with out func calls: {text}")

        var_rex = re.compile(r"\${([^}]+)}") if include_funcs else re.compile(r"\${[a-zA-Z_0-9]+(:[^}]+)?}")
        text_parts = []
        match = None
        while True:
            start = match.end() if match is not None else 0
            match = var_rex.search(text, start)
            if match is None:
                text_parts.append(text[start:])
                break

            text_parts.append(text[start : match.start()])
            var_name = match.string[match.start() : match.end()][2:-1]
            var_name_is_func = "(" in var_name and ":" not in var_name[: var_name.index("(")]
            self._log_replace_process(f"variable matched: var_name={var_name}, is_func={var_name_is_func}")
            if var_name_is_func:
                assert self.func_runner is not None
                var_value = self.func_runner.run_func(var_name, self)
            else:
                var_value = self._get_var_value(var_name, original_text)

            text_parts.append(str(var_value))
            self._log_replace_process(f"var_value: {var_value}")

        text = "".join(text_parts)
        if not comment_substituted:
            text = comment_substitutor.recover(text)
        self._log_replace_process(f"after variable replaced: text={text}")

        if original_text == text:
            return text
        return self._replace_variables(text, include_funcs=include_funcs, comment_substituted=comment_substituted)

    def _log_replace_process(self, message: str):
        if self.debug_log:
            import traceback

            f = traceback.extract_stack()[-2]
            logger.debug(f"({os.path.basename(f.filename)}:{f.lineno}) {message}")

    def add_vars(self, vars: Dict[str, str]):
        vars = {k.lower(): v for k, v in vars.items()}
        self.vars.update(vars)
        if self.debug_log:
            logger.debug(f"vars: {self.vars}")

    def add_list_vars(self, vars: Dict[str, List]):
        vars = {k.lower(): v for k, v in vars.items()}
        self.list_vars.update(vars)
        if self.debug_log:
            logger.debug(f"list vars: {self.vars}")


class TemplatesContext:
    def __init__(self, debug_log: bool = False, templates: Optional[Dict] = None):
        self.templates: Dict[str, str] = templates or {}
        self.debug_log = debug_log

    def replace_templates(self, text: str):
        comment_substitutor = CommentSubstitutor()
        text = comment_substitutor.substitute(text)

        templates = self.templates
        tmpl_with_arg_pattern = re.compile(r"@{\s*(\w+)\(\s*?(\s*\w+\s*=\s*[^,)]+\s*,?\s*)*\)\s*}", flags=re.IGNORECASE)
        tmpl_no_arg_pattern = re.compile(r"@{\s*(\w+)\s*}", flags=re.IGNORECASE)
        # pattern = re.compile(r'(%s)|(%s)' % (tmpl_with_arg_rex, tmpl_no_arg_rex), flags=re.IGNORECASE)
        while tmpl_with_arg_pattern.search(text) or tmpl_no_arg_pattern.search(text):
            match_result = tmpl_with_arg_pattern.search(text) or tmpl_no_arg_pattern.search(text)
            template_define = match_result.group(0)  # type: ignore
            self._log_replace_process(f"found template: `{template_define}`")
            template_define_normalized = template_define.replace("\n", "")
            template_name = match_result.groups()[0]  # type: ignore
            if template_name not in templates:
                raise SqlProcessorException(f"no template for found `{template_name}`, existing are {templates}")

            template = templates.get(template_name)
            assert template is not None, f"Content of template `{template_name}` is None, this should not happen!"
            template = template.strip()
            template_lines = template.split("\n")
            if comment_start(template_lines[-1]) != -1:
                # last line contains comment, add a new line to ensure it does not affect the referencing sql
                template = template + "\n"
            values = re.compile(r"\s*\w+\s*=\s*[^,)]+,?\s*", flags=re.IGNORECASE).findall(template_define_normalized)
            if values:
                index = 0
                while index < len(values):
                    value_def = str(values[index]).split("=")
                    value_name = value_def[0].strip()
                    value = value_def[1].replace(",", "").strip()
                    # fix for the last template parameter
                    if re.compile(r"\)}$").search(value):
                        value = value[:-2].strip()
                    self._log_replace_process(
                        f"template param matched: value_name={value_name}, value: {value}, template_name:"
                        f" {template_name}, template: {template}"
                    )
                    template = re.sub(re.escape(f"#{{{value_name}}}"), value, template, flags=re.IGNORECASE)
                    index += 1
            # sometimes there will be issue when some special characters are in the template(e.g. `\s`),
            # this could usually happen in comment
            #   Traceback (most recent call last):
            #       File "/usr/local/lib/python3.7/sre_parse.py", line 1015, in parse_template
            #           this = chr(ESCAPES[this][1])
            #       KeyError: '\\s'
            # to reproduce: re.sub(re.escape('@{streaming_state_indicator_meta_cols}'), '\s', 'abc')
            # change to simple text replacement to avoid this issue
            # the previous version is:
            #   text = re.sub(re.escape(template_define), template, text, flags=re.IGNORECASE)
            # to debug this:
            # print(f"template: `{template}`, template_define: `{template_define}`, text: `{text}`")
            text = text.replace(template_define, template)
            self._log_replace_process(f"text after template replaced: {text}")

            # recover the comment and substitute again to ensure no comment after template replacement
            text = comment_substitutor.recover(text)
            comment_substitutor = CommentSubstitutor()
            text = comment_substitutor.substitute(text)

        text = comment_substitutor.recover(text)
        self._log_replace_process(f"text after template replaced: {text}")

        return text

    def _log_replace_process(self, message: str):
        if self.debug_log:
            logger.debug(message)

    def add_templates(self, templates: Dict[str, str]):
        templates = {k.lower(): v for k, v in templates.items()}
        self.templates.update(templates)
        if self.debug_log:
            logger.debug(f"templates: {self.templates}")


class ProcessorContext:
    def __init__(
        self, vars_context: VarsContext, templates_context: TemplatesContext, extra_cols: Optional[List[Column]] = None
    ):
        self.vars_context = vars_context
        self.templates_context = templates_context
        self.extra_cols = extra_cols or []

    def replace_templates(self, text: str):
        return self.templates_context.replace_templates(text)

    def replace_variables(self, text: str) -> str:
        return self.vars_context.replace_variables(text)

    @property
    def vars(self):
        return self.vars_context.vars

    def set_vars(self, vars: Dict[str, Any]):
        self.vars_context.vars = vars

    def add_vars(self, vars: Dict[str, Any]):
        self.vars_context.add_vars(vars)

    def add_list_vars(self, list_vars: Dict[str, List]):
        self.vars_context.add_list_vars(list_vars)

    def add_templates(self, templates: Dict[str, str]):
        self.templates_context.add_templates(templates)
