import re

from sqlfluff.core import Lexer, Parser, Linter
from sqlfluff.core.config import FluffConfig
from sqlfluff.core.parser import CodeSegment

from easy_sql.logger import logger
from easy_sql.sql_linter.rules.bq_schema_rule import Rule_BigQuery_L001
from easy_sql.sql_processor.backend import Backend
from easy_sql.sql_processor.funcs import FuncRunner
from easy_sql.sql_processor.report import SqlProcessorReporter
from easy_sql.sql_processor.step import StepFactory, Step
from typing import Sequence
from sqlfluff.core.parser.segments import BaseSegment
from easy_sql.sql_processor.context import ProcessorContext, VarsContext, TemplatesContext
from typing import Dict, Any


def log_result(lint_result):
    for stage_result in lint_result:
        logger.info(stage_result.__repr__())


class SqlLinter:
    def __init__(self, sql: str):
        self.sql = sql
        self.supported_backend = ['spark', 'postgres', 'clickhouse', 'bigquery']
        self.backend = self._parse_backend(self.sql)
        self.step_list = self._get_step_list()
        self.include_rules = None
        self.exclude_rules = None
        self.context = self.get_context()
        self.all_rule_check_flag = False

    def get_context(self, variables: Dict[str, Any] = None):
        log_var_tmpl_replace = False
        vars_context = VarsContext(debug_log=log_var_tmpl_replace, vars=variables)
        func_runner = FuncRunner.create(Backend())
        vars_context.init(func_runner)
        return ProcessorContext(vars_context, TemplatesContext(debug_log=log_var_tmpl_replace, templates=None),
                                extra_cols=[])

    def set_check_all_rules(self):
        # if set check all rules, the inclucde and exclude function will not work
        # this is the first priority setting.
        self.all_rule_check_flag = True

    def set_include_rules(self, rules: [str]):
        # Include have lower priority than exclude
        self.include_rules = rules

    def set_exclude_rules(self, rules: [str]):
        # Exclude have higher priority than include
        self.exclude_rules = rules

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
        return backend

    def _get_step_list(self):
        reporter = SqlProcessorReporter(report_task_id='sql_linter')
        func_runner = FuncRunner()
        step_factory = StepFactory(reporter, func_runner)
        step_list = step_factory.create_from_sql(self.sql)
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

    def _update_dialect_for_config(self, config):
        config['core']['dialect'] = self._get_dialect_from_backend()

    def _update_included_rule_for_config(self, config, context: str = "all", rules=None):
        if rules is None:
            rules = []
        if len(rules) > 0 and not self.all_rule_check_flag:
            config['core']['rules'] = ",".join(rules)
        elif self.all_rule_check_flag:
            config['core']['rules'] = "all"
        else:
            config['core']['rules'] = context

    def _update_excluded_rule_for_config(self, config, rules: [str] = None):
        if rules is not None and not self.all_rule_check_flag:
            config['core']['exclude_rules'] = ",".join(rules)
        else:
            config['core']['exclude_rules'] = None

    def check_lexable(self, tokens: Sequence[BaseSegment]):
        for token in tokens:
            if token.is_type("unlexable"):
                logger.warn("Query have unlexable segment: " + str(token.raw_segments))
                return False
        return True

    def parse_variable(self, tokens: Sequence[BaseSegment]):
        variable_dict = {}
        find_as_statement_flag = False
        for token in tokens:
            if find_as_statement_flag and isinstance(token, CodeSegment):
                find_as_statement_flag = False
                variable = token.raw
                variable_dict[variable] = variable

            if token.raw == "as":
                find_as_statement_flag = True
        return variable_dict

    def preprocess_select_sql_for_template(self, step: Step):
        try:
            step.replace_templates_and_mock_variables(self.context)
        except Exception as e:
            logger.warn("functions or variables replace fail is reasonable: " + str(e))

    def lint_step_sql(self, step: Step, linter: Linter, backend: str):
        self.preprocess_select_sql_for_template(step)
        if step.check_if_template_statement():
            step.add_template_to_context(self.context)
        else:
            sql = step.select_sql
            logger.info("check sql :" + sql)
            lexer = Lexer(dialect=self._get_dialect_from_backend(backend))
            parser = Parser(dialect=self._get_dialect_from_backend(backend))
            tokens, _ = lexer.lex(sql)
            if self.check_lexable(tokens):
                parsed = parser.parse(tokens)
                result = linter.lint(parsed)
                fixed_tree, violation = linter.fix(parsed)
                logger.info("after fix: " + fixed_tree.raw)
                return result

    def prepare_linter(self, backend):
        default_config_dict = FluffConfig(require_dialect=False)._configs
        self._update_dialect_for_config(default_config_dict)
        self._update_included_rule_for_config(default_config_dict,
                                              context=self._get_dialect_from_backend(backend),
                                              rules=self.include_rules)
        self._update_excluded_rule_for_config(default_config_dict, rules=self.exclude_rules)
        update_config = FluffConfig(configs=default_config_dict)
        linter = Linter(config=update_config, user_rules=[Rule_BigQuery_L001])
        return linter

    def lint(self, backend: str):
        lint_result = []
        linter = self.prepare_linter(backend)
        for step in self.step_list:
            step_lint_result = self.lint_step_sql(step, linter, backend)
            if step_lint_result:
                log_result(step_lint_result)
                lint_result = lint_result + step_lint_result
        return lint_result
