from typing import List, Any, Dict, Callable, Union

from .backend import Backend, SparkBackend
from .common import Column
from .context import ProcessorContext, VarsContext, TemplatesContext
from .funcs import FuncRunner
from .report import DataSqlProcessorReporter, StepStatus
from .step import Step, StepFactory
from ..logger import logger


def extract_funcs_from_pyfile(funcs_py_file):
    import os
    import sys
    import importlib
    sys.path.insert(0, os.path.dirname(funcs_py_file))
    func_mod = importlib.import_module(os.path.basename(funcs_py_file)[:-3])
    funcs = dict([(func, getattr(func_mod, func)) for func in dir(func_mod) if callable(getattr(func_mod, func))])
    return funcs


class SqlProcessor:

    def __init__(self, backend: Union['SparkSession', Backend], sql: str, extra_cols: List[Column] = None, variables: dict = None,
                 report_hdfs_path: str = None, report_task_id: str = None, report_es_url: str = None, report_es_index_prefix: str = None,
                 scala_udf_initializer: str = None, templates: dict = None, includes: Dict[str, str] = None):
        backend = backend if isinstance(backend, (Backend, )) else SparkBackend(spark=backend)
        self.backend = backend
        self.sql = sql

        self.reporter = DataSqlProcessorReporter(report_task_id=report_task_id, report_hdfs_path=report_hdfs_path,
                                                 report_es_url=report_es_url, report_es_index_prefix=report_es_index_prefix)
        log_var_tmpl_replace = False
        vars_context = VarsContext(debug_log=log_var_tmpl_replace, vars=variables)
        self.func_runner = FuncRunner.create(self.backend)
        vars_context.init(self.func_runner)
        self.context = ProcessorContext(vars_context, TemplatesContext(debug_log=log_var_tmpl_replace, templates=templates), extra_cols=extra_cols)
        self.step_factory = StepFactory(self.reporter, self.func_runner)

        self.step_list = self.step_factory.create_from_sql(self.sql, includes)
        self.reporter.init(self.step_list)
        self.backend.init_udfs(scala_udf_initializer=scala_udf_initializer)

    @property
    def variables(self) -> [str, Any]:
        return self.context.vars_context.vars

    @property
    def templates(self) -> Dict[str, str]:
        return self.context.templates_context.templates

    @property
    def extra_cols(self) -> List[Column]:
        return self.context.extra_cols

    def set_spark_configs(self, configs: Dict[str, str]):
        if self.backend.is_spark_backend:
            self.backend.set_spark_configs(configs)
        else:
            logger.warn(f'ignored set spark configs when backend is of type {type(self.backend)}')

    def register_funcs_from_pyfile(self, funcs_py_file: str):
        funcs = extract_funcs_from_pyfile(funcs_py_file)
        self.register_funcs(funcs)

    def register_funcs(self, funcs: Dict[str, Callable]):
        self.func_runner.register_funcs(funcs)

    def register_udfs_from_pyfile(self, funcs_py_file: str):
        logger.info(f'resolving udfs from pyfile {funcs_py_file}')
        funcs = extract_funcs_from_pyfile(funcs_py_file)
        self.register_udfs(funcs)

    def register_udfs(self, funcs: Dict[str, Callable]):
        self.backend.register_udfs(funcs)

    def set_vars(self, vars: Dict[str, Any]):
        self.context.set_vars(vars)

    def add_vars(self, vars: Dict[str, Any]):
        self.context.add_vars(vars)

    def run_step(self, step: Step, dry_run: bool):
        try:
            # add meta vars to support step information retrieving in functions
            self.context.add_vars({'__step__': step})
            self.context.add_vars({'__context__': self.context})
            if not step.should_run(self.context):
                self.reporter.collect_report(step, status=StepStatus.SKIPPED)
                return
            self.reporter.collect_report(step, status=StepStatus.RUNNING)
            df = step.read(self.backend, self.context)
            step.write(self.backend, df, self.context, dry_run)
            self.reporter.collect_report(step, status=StepStatus.SUCCEEDED)
        except Exception as e:
            import traceback
            self.reporter.collect_report(step, status=StepStatus.FAILED, message=traceback.format_exc())
            if '__exception_handler__' in self.variables and str(self.variables['__exception_handler__']).upper() != 'NULL':
                func_name: str = self.variables['__exception_handler__']
                func_name = func_name.replace('{', '${')
                self.func_runner.run_func(func_name, self.context.vars_context)(e)
            else:
                raise e

    def run(self, dry_run: bool = False):
        try:
            for step in self.step_list:
                self.run_step(step, dry_run)
        finally:
            self.reporter.print_report(True)
