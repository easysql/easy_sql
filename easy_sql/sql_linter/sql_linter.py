from __future__ import annotations

import re
from typing import TYPE_CHECKING, List, Optional, Sequence

import regex
from sqlfluff.core import Lexer, Linter, Parser, SQLBaseError
from sqlfluff.core.config import FluffConfig
from sqlfluff.core.parser import CodeSegment, RegexLexer

from easy_sql.sql_linter.rules import all_rules
from easy_sql.sql_linter.sql_linter_reportor import LintReporter
from easy_sql.sql_processor.funcs import FuncRunner
from easy_sql.sql_processor.report import SqlProcessorReporter
from easy_sql.sql_processor.step import Step, StepFactory

if TYPE_CHECKING:
    from sqlfluff.core.parser.segments import BaseSegment


class SqlLinter:
    def __init__(self, sql: str, include_rules: Optional[List[str]] = None, exclude_rules: Optional[List[str]] = None):
        self.origin_sql = sql
        self.fixed_sql_list = []
        self.supported_backend = ["spark", "postgres", "clickhouse", "bigquery", "flink"]
        self.step_list = self._get_step_list()
        self.include_rules = include_rules
        self.exclude_rules = exclude_rules
        self.reporter = LintReporter()

    def _parse_backend(self, sql: str):
        sql_lines = sql.split("\n")
        backend = None
        for line in sql_lines:
            if re.match(r"^-- \s*backend:.*$", line):
                backend = line[line.index("backend:") + len("backend:") :].strip()
                break

        if backend is None:
            backend = "spark"
            self.reporter.report_warning("Backend cannot be found in sql, will use default backend spark")

        if backend not in self.supported_backend:
            raise Exception(
                f"Unsupported backend `${backend}`, all supported backends are: " + ",".join(self.supported_backend)
            )

        self.reporter.report_message(f"Use backend: {backend}")
        self.fixed_sql_list.append("-- backend: " + backend)
        return backend

    def _get_step_list(self):
        reporter = SqlProcessorReporter(report_task_id="sql_linter")
        func_runner = FuncRunner()
        step_factory = StepFactory(reporter, func_runner)
        step_list = step_factory.create_from_sql(self.origin_sql)
        return step_list

    def _get_dialect_from_backend(self, backend: Optional[str] = None) -> str:
        backend = backend or self._parse_backend(self.origin_sql)
        if backend == "spark":
            return "sparksql"
        if backend == "bigquery":
            return "bigquery"
        if backend == "clickhouse":
            # TODO: so far do not have clickhouse in sql fluff
            return "ansi"
        if backend == "postgres":
            return "postgres"
        raise Exception("backend type so far is not supported for lint check")

    @staticmethod
    def _included_rule_config(dialect: Optional[str] = None, rules=None) -> str:
        if rules is None:
            rules = []
        if len(rules) > 0:
            return ",".join(rules)
        else:
            if dialect in ["bigquery"]:
                return "core,L019," + dialect
            else:
                return "core,L019"

    @staticmethod
    def _excluded_rule_config(rules: Optional[List[str]] = None) -> Optional[str]:
        if rules is not None:
            return ",".join(rules)
        else:
            return None

    def _check_parsable(self, root_segment: BaseSegment) -> bool:
        segment_list = [root_segment]
        while len(segment_list) > 0:
            check_segment = segment_list[0]
            segment_list.remove(check_segment)
            if check_segment.is_type("unparsable"):
                self.reporter.report_warning("Unparsable segment found: " + check_segment.raw)
                return False
            elif check_segment.segments:
                segment_list = segment_list + list(check_segment.segments)
        return True

    def _check_lexable(self, tokens: Sequence[BaseSegment]) -> bool:
        for _, token in enumerate(tokens):
            if token.is_type("unlexable"):
                self.reporter.report_warning("Unlexable segment found: " + str(token.raw_segments))
                return False
        return True

    def _lint_step_sql(
        self, step: Step, lexer: Lexer, parser: Parser, linter: Linter, log_error: bool
    ) -> List[SQLBaseError]:
        assert step.target_config is not None
        self.fixed_sql_list.append(step.target_config.step_config_str)
        vialations = []
        if step.select_sql:
            if step.is_template_statement():
                self.fixed_sql_list.append(step.select_sql + "\n")
                self.reporter.report_message("Skip template sql for this step.")
            else:
                sql = step.select_sql + "\n"
                tokens, _ = lexer.lex(sql)
                parsed = parser.parse(tokens)
                assert parsed is not None

                if self._check_lexable(tokens) and self._check_parsable(parsed):
                    result = linter.lint(parsed)
                    if log_error:
                        self.reporter.report_list_of_violations(result, step.target_config.line_no)
                    fixed_tree, violation = linter.fix(parsed)
                    vialations += violation
                    self.fixed_sql_list.append(fixed_tree.raw)
                    return result
                else:
                    self.fixed_sql_list.append(step.select_sql)
        return vialations

    def _prepare_lexer(self, dialect: str):
        lexer = Lexer(dialect=dialect)

        # Add regex mather to lexer to enable easy sql
        easy_sql_function = RegexLexer("easy_sql_function", r"\${[^\s,]+\(.+\)}", CodeSegment)
        easy_sql_variable = RegexLexer("easy_sql_variable", r"\${[^\s,]+}", CodeSegment)
        easy_sql_template = RegexLexer("easy_sql_template", r"@{[^{]+}", CodeSegment)
        three_quote_string = RegexLexer("three_quote_string", r'""".*"""', CodeSegment)
        lexer.lexer_matchers.insert(0, easy_sql_variable)
        lexer.lexer_matchers.insert(0, easy_sql_function)
        lexer.lexer_matchers.insert(0, easy_sql_template)
        lexer.lexer_matchers.insert(0, three_quote_string)
        return lexer

    def _prepare_parser(self, dialect: str):
        parser = Parser(dialect=dialect)
        # Let parser recognize all function/variables/template into nakedIdentifier
        # which will be given grammar when given context
        identifier_segement = parser.config.get("dialect_obj")._library["NakedIdentifierSegment"]
        template = identifier_segement.template + r"|@{[^{]+}|[\$]{[\s\S]+}|\"[\s\S]+\""
        identifier_segement.template = template
        # For sqlfluff > 1.0, template is compiled and cached, so we need to update it as well.
        if identifier_segement._template is not None:
            identifier_segement._template = regex.compile(template, regex.IGNORECASE)
        return parser

    def _lint_for_easy_sql(
        self, backend: str, log_error: bool = True, config_path: Optional[str] = None
    ) -> List[SQLBaseError]:
        lint_result = []
        dialect = self._get_dialect_from_backend(backend)
        lexer = self._prepare_lexer(dialect)
        parser = self._prepare_parser(dialect)
        linter = self._prepare_linter(dialect, config_path)
        step_count = 0
        self.fixed_sql_list = self._parser_sql_header()
        for step_no, step in enumerate(self.step_list):
            step_count = step_count + 1
            if log_error:
                assert step.target_config is not None
                self.reporter.report_message(
                    "=== Check step {} at line {} ===".format(step_no + 1, step.target_config.line_no)
                )
            step_result = self._lint_step_sql(step, lexer, parser, linter, log_error)
            if step_result:
                lint_result = lint_result + step_result
        return lint_result

    def _lint_for_normal_sql(
        self, backend: str, log_error: bool = True, config_path: Optional[str] = None
    ) -> List[SQLBaseError]:
        self.fixed_sql_list = []
        dialect = self._get_dialect_from_backend(backend)
        lexer = Lexer(dialect=dialect)
        parser = Parser(dialect=dialect)
        linter = self._prepare_linter(dialect, config_path=config_path)
        tokens, _ = lexer.lex(self.origin_sql)
        parsed = parser.parse(tokens)
        assert parsed is not None
        lint_result = linter.lint(parsed)
        fixed_tree, violation = linter.fix(parsed)
        if log_error:
            self.reporter.report_list_of_violations(lint_result)
        self.fixed_sql_list.append(fixed_tree.raw)
        return lint_result

    def _parser_sql_header(self):
        assert self.step_list[0].target_config is not None
        line_no = self.step_list[0].target_config.line_no
        return self.origin_sql.split("\n")[: line_no - 1]

    def _prepare_linter(self, dialect: str, config_path: Optional[str] = None):
        if config_path:
            config = FluffConfig.from_path(config_path)
        else:
            config = {
                "core": {
                    "dialect": dialect,
                    "rules": self._included_rule_config(dialect=dialect, rules=self.include_rules),
                    "exclude_rules": self._excluded_rule_config(rules=self.exclude_rules),
                },
                "layout": {"type": {"comma": {"line_position": "leading"}}},
                "rules": {"L014": {"extended_capitalisation_policy": "lower"}},
            }
            config = FluffConfig(configs=config)
        linter = Linter(config=config, user_rules=all_rules)  # type: ignore
        return linter

    def lint(
        self, backend: str, log_error: bool = True, easysql: bool = True, config_path: Optional[str] = None
    ) -> List[SQLBaseError]:
        if easysql:
            return self._lint_for_easy_sql(backend, log_error, config_path=config_path)
        else:
            return self._lint_for_normal_sql(backend, log_error, config_path=config_path)

    def fix(
        self, backend: str, log_linter_error: bool = False, easy_sql: bool = True, config_path: Optional[str] = None
    ):
        self.lint(backend, log_linter_error, easy_sql, config_path=config_path)
        return "\n".join(self.fixed_sql_list)
