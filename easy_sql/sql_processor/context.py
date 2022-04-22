import copy
import re
from typing import Dict, Any, List

from ..logger import logger
from . import FuncRunner
from .common import SqlProcessorException, Column, VarsReplacer

__all__ = [
    'VarsContext', 'TemplatesContext', 'ProcessorContext'
]


class CommentSubstitutor:
    COMMENT_IDENTIFIABLE_NAME = '__COMMENT_SUBSTITUTED__'

    def __init__(self):
        self.recognized_comment = []

    def substitute(self, sql_expr: str) -> str:
        if CommentSubstitutor.COMMENT_IDENTIFIABLE_NAME in sql_expr:
            raise Exception(f'Cannot handle sql expression with '
                            f'comment identifiable name({CommentSubstitutor.COMMENT_IDENTIFIABLE_NAME}) inside: {sql_expr}')
        lines = []
        recognized_comment = []
        for line in sql_expr.split('\n'):
            if line.startswith('-- '):
                lines.append(CommentSubstitutor.COMMENT_IDENTIFIABLE_NAME + str(len(recognized_comment)) + '__')
                recognized_comment.append(line)
                continue

            current_index = 0
            while True:
                m = re.match(r'.*?([^a-zA-Z0-9_]-- ).*', line[current_index:])
                if m:
                    comment_start = m.start(1) + 1
                    left_of_comment = line[:current_index + comment_start]
                    if CommentSubstitutor.is_quote_closed(left_of_comment):
                        lines.append(left_of_comment + CommentSubstitutor.COMMENT_IDENTIFIABLE_NAME + str(len(recognized_comment)) + '__')
                        recognized_comment.append(line[current_index + comment_start:])
                        break
                    else:
                        current_index += comment_start
                else:
                    lines.append(line)
                    break

        self.recognized_comment = recognized_comment
        return '\n'.join(lines)

    @staticmethod
    def is_quote_closed(sql_expr: str) -> bool:
        sql_expr = sql_expr.replace('\\\\', '')

        def find_char_without_escape(text: str, char: str) -> int:
            start = 0
            while True:
                idx = text.find(char, start)
                if idx != -1:
                    if idx > 0 and text[idx - 1] == '\\':
                        start = idx + 1
                    else:
                        return idx
                else:
                    return -1

        def _is_quote_closed(quote_index: int, quote_char: str) -> bool:
            start_index = quote_index + 1
            char_without_escape_idx = find_char_without_escape(sql_expr[start_index:], quote_char)
            if char_without_escape_idx == -1:
                return False
            else:
                return CommentSubstitutor.is_quote_closed(sql_expr[start_index + char_without_escape_idx + 1:])

        # a valid sql_expr should not starts with \' or \", so we don't need to check \\ when find quote start point
        single_quote_index = sql_expr.find("'")
        double_quote_index = sql_expr.find('"')
        if single_quote_index != -1 and double_quote_index == -1:
            return _is_quote_closed(single_quote_index, "'")
        elif single_quote_index != -1 and double_quote_index != -1:
            if single_quote_index < double_quote_index:
                return _is_quote_closed(single_quote_index, "'")
            else:
                return _is_quote_closed(double_quote_index, '"')
        elif single_quote_index == -1 and double_quote_index == -1:
            return True
        elif single_quote_index == -1 and double_quote_index != -1:
            return _is_quote_closed(double_quote_index, '"')

    def recover(self, substituted_sql_expr: str) -> str:
        lines = []
        comment_index = 0
        for line in substituted_sql_expr.split('\n'):
            identifiable_name = f'{CommentSubstitutor.COMMENT_IDENTIFIABLE_NAME}{comment_index}__'
            identifiable_name_count = line.count(identifiable_name)
            if identifiable_name_count > 1:
                raise Exception(f'found multiple comment identifiable name {identifiable_name} in line: {line}')
            if identifiable_name_count == 1 and not line.endswith(identifiable_name):
                raise Exception(f'found comment identifiable name {identifiable_name}, but is not at end in line: {line}')
            if identifiable_name_count == 1:
                lines.append(line.replace(identifiable_name, self.recognized_comment[comment_index]))
                comment_index += 1
            else:
                lines.append(line)
        return '\n'.join(lines)


class VarsContext(VarsReplacer):

    def __init__(self, vars: Dict[str, Any] = None, debug_log: bool = False):
        vars = vars or {}
        self.vars = copy.deepcopy(vars)
        self.list_vars = {}
        self.func_runner = None
        self.debug_log = debug_log

    def init(self, func_runner: 'FuncRunner'):
        self.func_runner = func_runner

    def replace_variables(self, text: str, include_funcs: bool = True) -> str:
        m = re.match(r'^\${([^}]+)}$', text.strip())
        if m:
            return self.vars.get(m.group(1).strip(), self.vars.get(m.group(1).strip().lower()))

        original_text = text
        comment_substitutor = CommentSubstitutor()
        text = comment_substitutor.substitute(text)

        variables = self.vars
        for key in variables.keys():
            text = re.sub(re.escape(f'${{{key}}}'), str(variables.get(key)), text, flags=re.IGNORECASE)
        self._log_replace_process(f'after direct variable replaced: {text}')

        if not include_funcs:
            return comment_substitutor.recover(text)

        var_rex = re.compile(r'\${([^}]+)}')
        text_parts = []
        match = None
        while True:
            start = match.end() if match is not None else 0
            match = var_rex.search(text, start)
            if match is None:
                text_parts.append(text[start:])
                break

            text_parts.append(text[start:match.start()])
            var_name = var_rex.match(match.group()).groups()[0]
            var_name_is_func = '(' in var_name
            self._log_replace_process(f'variable matched: var_name={var_name}, is_func={var_name_is_func}')
            if var_name_is_func:
                var_value = self.func_runner.run_func(var_name, self)
            elif var_name in variables:
                var_value = variables[var_name]
            elif var_name.upper() in variables:
                var_value = variables[var_name.upper()]
            else:
                raise SqlProcessorException(f'unknown variable `{var_name}`. text={original_text}, known_vars={variables}')

            text_parts.append(str(var_value))
            self._log_replace_process(f'var_value: {var_value}')

        text = ''.join(text_parts)
        text = comment_substitutor.recover(text)
        self._log_replace_process(f'after variable replaced: text={text}')
        return text

    def _log_replace_process(self, message: str):
        if self.debug_log:
            logger.debug(message)

    def add_vars(self, vars: Dict[str, str]):
        vars = {k.lower(): v for k, v in vars.items()}
        self.vars.update(vars)
        if self.debug_log:
            logger.debug(f'vars: {self.vars}')

    def add_list_vars(self, vars: Dict[str, List]):
        vars = {k.lower(): v for k, v in vars.items()}
        self.list_vars.update(vars)
        if self.debug_log:
            logger.debug(f'list vars: {self.vars}')


class TemplatesContext:

    def __init__(self, debug_log: bool = False, templates: dict = None):
        self.templates: Dict[str, str] = templates or {}
        self.debug_log = debug_log

    def replace_templates(self, text: str):
        templates = self.templates
        tmpl_with_arg_pattern = re.compile(r'@{\s*(\w+)\(\s*?(\s*\w+\s*=\s*[^,)]+\s*,?\s*)*\)\s*}', flags=re.IGNORECASE)
        tmpl_no_arg_pattern = re.compile(r'@{\s*(\w+)\s*}', flags=re.IGNORECASE)
        # pattern = re.compile(r'(%s)|(%s)' % (tmpl_with_arg_rex, tmpl_no_arg_rex), flags=re.IGNORECASE)
        while tmpl_with_arg_pattern.search(text) or tmpl_no_arg_pattern.search(text):
            match_result = tmpl_with_arg_pattern.search(text) or tmpl_no_arg_pattern.search(text)
            template_define = match_result.group(0)
            self._log_replace_process(f'found template: {template_define}')
            template_define_normalized = template_define.replace('\n', '')
            template_name = match_result.groups()[0]
            if template_name not in templates:
                raise SqlProcessorException(f'no template for found `{template_name}`, existing are {templates}')

            template = templates.get(template_name)
            values = re.compile(r'\s*\w+\s*=\s*[^,)]+,?\s*', flags=re.IGNORECASE).findall(template_define_normalized)
            if values:
                index = 0
                while index < len(values):
                    value_def = str(values[index]).split('=')
                    value_name = value_def[0].strip()
                    value = value_def[1].replace(',', '').strip()
                    # fix for the last template parameter
                    if re.compile(r'\)}$').search(value):
                        value = value[:-2].strip()
                    self._log_replace_process(f'template param matched: value_name={value_name}, value: {value}, template_name: {template_name}, template: {template}')
                    template = re.sub(re.escape(f'#{{{value_name}}}'), value, template, flags=re.IGNORECASE)
                    index += 1
            text = re.sub(re.escape(template_define), template, text, flags=re.IGNORECASE)
            self._log_replace_process(f'text after template replaced: {text}')

        return text

    def _log_replace_process(self, message: str):
        if self.debug_log:
            logger.debug(message)

    def add_templates(self, templates: Dict[str, str]):
        templates = {k.lower(): v for k, v in templates.items()}
        self.templates.update(templates)
        if self.debug_log:
            logger.debug(f'templates: {self.templates}')


class ProcessorContext:

    def __init__(self, vars_context: VarsContext, templates_context: TemplatesContext, extra_cols: List[Column] = None):
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
