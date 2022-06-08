import re

from sqlfluff.core import Lexer, Parser, Linter
from sqlfluff.core.config import FluffConfig

from easy_sql.logger import logger
from easy_sql.sql_linter.rules import __all__
from easy_sql.sql_processor.backend import Backend
from easy_sql.sql_processor.funcs import FuncRunner
from easy_sql.sql_processor.report import SqlProcessorReporter
from easy_sql.sql_processor.step import StepFactory, Step
from typing import Sequence
from sqlfluff.core.parser.segments import BaseSegment
from easy_sql.sql_processor.context import ProcessorContext, VarsContext, TemplatesContext
from sqlfluff.core.parser import RegexLexer,CodeSegment

class VarNameDict(list):
    def __getitem__(self, item):
        return f'__{item}__'


def log_result(lint_result):
    for stage_result in lint_result:
        logger.info(stage_result.__repr__())


class SqlLinter:
    def __init__(self, sql: str, include_rules: [str] = None, exclude_rules: [str] = None):
        self.origin_sql = sql
        self.fixed_sql_list = []
        self.supported_backend = ['spark', 'postgres', 'clickhouse', 'bigquery']
        self.backend = self._parse_backend(self.origin_sql)
        self.step_list = self._get_step_list()
        self.include_rules = include_rules
        self.exclude_rules = exclude_rules
        self.context = self._get_context()

    @staticmethod
    def _get_context():
        log_var_tmpl_replace = False
        vars_context = VarsContext(debug_log=log_var_tmpl_replace)
        func_runner = FuncRunner.create(Backend())
        vars_context.init(func_runner)
        return ProcessorContext(vars_context, TemplatesContext(debug_log=log_var_tmpl_replace, templates=None),
                                extra_cols=[])

    def _parse_backend(self, sql: str):
        sql_lines = sql.split('\n')
        backend = None
        for line in sql_lines:
            if re.match(r'^-- \s*backend:.*$', line):
                backend = line[line.index('backend:') + len('backend:'):].strip()
                break

        if backend is None:
            backend = "spark"
            logger.warn("Backend cannot be found in sql, will use default backend spark")

        if backend not in self.supported_backend:
            raise Exception(
                f'Unsupported backend `${backend}`, all supported backends are: ' + ",".join(self.supported_backend))

        logger.info(f"Use backend: {backend}")
        self.fixed_sql_list.append("-- backend: " + backend)
        return backend

    def _get_step_list(self):
        reporter = SqlProcessorReporter(report_task_id='sql_linter')
        func_runner = FuncRunner()
        step_factory = StepFactory(reporter, func_runner)
        step_list = step_factory.create_from_sql(self.origin_sql)
        return step_list

    def _get_dialect_from_backend(self, backend: str = None):
        backend = backend or self.backend
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

    def _update_dialect_for_config(self, config, dialect: str):
        config['core']['dialect'] = dialect

    @staticmethod
    def _update_included_rule_for_config(config, context: str = "all", rules=None):
        if rules is None:
            rules = []
        if len(rules) > 0:
            config['core']['rules'] = ",".join(rules)
        else:
            config['core']['rules'] = "core," + context

    @staticmethod
    def _update_excluded_rule_for_config(config, rules: [str] = None):
        if rules is not None:
            config['core']['exclude_rules'] = ",".join(rules)
        else:
            config['core']['exclude_rules'] = None

    @staticmethod
    def _check_lexable(tokens: Sequence[BaseSegment]):
        for i, token in enumerate(tokens):
            # TODO:create new
            if token.is_type("unlexable"):
                logger.warn("Query have unlexable segment，currently function are not support: "
                            + str(token.raw_segments))
                return False
        return True

    def _lint_step_sql(self, step: Step, linter: Linter, backend: str, log_error: bool):
        self.fixed_sql_list.append(step.target_config.step_config_str)
        if step.select_sql:
            print("before: " + step.select_sql)
            if step.check_if_template_statement():
                self.fixed_sql_list.append(step.select_sql)
                step.add_template_to_context(self.context)
            else:
                sql = step.select_sql
                lexer = Lexer(dialect=self._get_dialect_from_backend(backend))
                easy_sql_function = RegexLexer('easy_dollar_quote', r'\${[^\s,]+\(.+\)}', CodeSegment)
                easy_sql_variable = RegexLexer('easy_dollar_quote', r'\${[^\s,]+}', CodeSegment)
                easy_sql_template = RegexLexer('easy_at_quote', r'@{[^\s,]+}', CodeSegment)
                three_quote_regrex = RegexLexer('three_quote_regrex', r'""".*"""', CodeSegment)
                lexer.lexer_matchers.insert(0, easy_sql_variable)
                lexer.lexer_matchers.insert(0, easy_sql_function)
                lexer.lexer_matchers.insert(0, easy_sql_template)
                lexer.lexer_matchers.insert(0, three_quote_regrex)
                parser = Parser(dialect=self._get_dialect_from_backend(backend))
                identifier_segement = parser.config.get("dialect_obj")._library["NakedIdentifierSegment"]
                identifier_segement.template = r"[\"@$A-Z_][\"${}A-Z0-9_]*"

                tokens, _ = lexer.lex(sql)
                if self._check_lexable(tokens):
                    parsed = parser.parse(tokens)
                    result = linter.lint(parsed)
                    if log_error:
                        log_result(result)
                    fixed_tree, violation = linter.fix(parsed)
                    self.fixed_sql_list.append(fixed_tree.raw)
                    print("after: " + fixed_tree.raw)
                    return result
                else:
                    self.fixed_sql_list.append(step.select_sql)

    def prepare_linter(self, dialect):
        default_config_dict = FluffConfig(require_dialect=False)._configs
        default_config_dict['rules']['L019'] = {'comma_style': 'leading'}
        self._update_dialect_for_config(default_config_dict, dialect)
        self._update_included_rule_for_config(default_config_dict,
                                              context=dialect,
                                              rules=self.include_rules)
        self._update_excluded_rule_for_config(default_config_dict, rules=self.exclude_rules)
        update_config = FluffConfig(configs=default_config_dict)
        linter = Linter(config=update_config, user_rules=__all__)
        return linter

    def lint(self, backend: str, log_error: bool = True):
        lint_result = []
        dialect = self._get_dialect_from_backend(backend)
        linter = self.prepare_linter(dialect)
        step_count = 0
        self.fixed_sql_list = self._parser_sql_header()
        for step in self.step_list:
            step_count = step_count + 1
            logger.info('currently check: ' + str(step_count))
            step_result = self._lint_step_sql(step, linter, backend, log_error)
            self.fixed_sql_list.append("")
            if step_result:
                lint_result = lint_result + step_result
        return lint_result

    def _parser_sql_header(self):
        line_no = self.step_list[0].target_config.line_no
        return self.origin_sql.split("\n")[:line_no - 1]

    def fix(self, backend: str, log_linter_error: bool = True):
        self.lint(backend, log_linter_error)
        delimiter = "\n"
        # TODO: need all revert to previous logic
        reunion_sql = delimiter + delimiter.join(self.fixed_sql_list)
        var_dict = self.context.vars_context.vars
        print(var_dict)
        # for variable_key in var_dict:
        #     replace_string = var_dict[variable_key]
        #     reunion_sql = reunion_sql.replace(replace_string, "${"+variable_key+"}")
        return reunion_sql
