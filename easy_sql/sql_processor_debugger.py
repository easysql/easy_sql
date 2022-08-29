from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from .sql_processor import Column, SqlProcessor, Step

__all__ = ["SqlProcessorDebugger"]

from .sql_processor.backend import Backend, SparkBackend

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class SqlProcessorDebugger:
    def __init__(
        self,
        sql_file_path: str,
        backend: Union[SparkSession, Backend],
        vars: Optional[Dict[str, Any]] = None,
        funcs: Optional[Dict[str, Any]] = None,
        funcs_py_file: Optional[str] = None,
        extra_cols: Optional[List[Column]] = None,
        udf_py_file: Optional[str] = None,
        scala_udf_initializer: Optional[str] = None,
        templates: Optional[Dict[str, Any]] = None,
    ):
        backend = backend if isinstance(backend, (Backend,)) else SparkBackend(spark=backend)
        self.udf_py_file = udf_py_file
        self.sql_file_path = sql_file_path
        self.scala_udf_initializer = scala_udf_initializer
        self.initial_vars, self.initial_funcs, self.funcs_py_file, self.initial_extra_cols, self.initial_templates = (
            vars or {},
            funcs or {},
            funcs_py_file,
            extra_cols or [],
            templates or {},
        )
        self.backend = backend
        self.sql_processor = self._create_sql_processor()
        self.steps = self.sql_processor.step_list
        self._current_step_index = -1
        self.initial_temp_views = self.tempviews

    def _create_sql_processor(self) -> SqlProcessor:
        import copy

        with open(self.sql_file_path, "r") as f:
            sql = f.read()
        sql_processor = SqlProcessor(
            self.backend,
            sql,
            extra_cols=copy.deepcopy(self.initial_extra_cols),
            variables=copy.deepcopy(self.initial_vars),
            scala_udf_initializer=self.scala_udf_initializer,
            templates=copy.deepcopy(self.initial_templates),
        )
        if self.initial_funcs:
            sql_processor.register_funcs(self.initial_funcs)
        if self.funcs_py_file:
            sql_processor.register_funcs_from_pyfile(self.funcs_py_file)
        if self.udf_py_file:
            sql_processor.register_udfs_from_pyfile(self.udf_py_file)
        return sql_processor

    @property
    def is_started(self) -> bool:
        return self._current_step_index > -1

    @property
    def is_inprogress(self) -> bool:
        return -1 < self._current_step_index < len(self.steps) - 1

    @property
    def is_finished(self):
        return self._current_step_index == len(self.steps) - 1

    @property
    def current_step(self) -> Optional[Step]:
        if self._current_step_index < len(self.steps) and self._current_step_index != -1:
            return self.steps[self._current_step_index]
        return None

    @property
    def current_step_no(self) -> Optional[int]:
        if self._current_step_index < len(self.steps) and self._current_step_index != -1:
            return self._current_step_index + 1
        if self._current_step_index == -1:
            print("Not started yet! No current step number right now.")
        else:
            print("Already finished! No current step number right now.")

    @property
    def next_step(self) -> Optional[Step]:
        if self._current_step_index < len(self.steps) - 1:
            return self.steps[self._current_step_index + 1]
        return None

    @property
    def next_step_no(self) -> Optional[int]:
        step_index = self._current_step_index + 1
        if step_index < len(self.steps) and step_index != -1:
            return step_index + 1
        else:
            print("Already finished! No next step number right now.")

    @property
    def last_step(self) -> Optional[Step]:
        if self._current_step_index > 0:
            return self.steps[self._current_step_index - 1]
        return None

    @property
    def last_step_no(self) -> Optional[int]:
        step_index = self._current_step_index - 1
        if step_index == -2:
            print("Not started yet! No last step number right now.")
            return None
        if step_index < len(self.steps) and step_index != -1:
            return step_index + 1

    @property
    def left_step_count(self) -> int:
        return len(self.steps) - 1 - self._current_step_index

    @property
    def vars(self) -> Dict[str, Any]:
        return self.sql_processor.variables

    def add_vars(self, vars: Optional[Dict[str, Any]]):
        if vars is None or not isinstance(vars, dict):
            print("Vars must be a non-empty dict. Will do nothing!")
            return
        self.sql_processor.add_vars(vars)
        self.initial_vars.update(vars)

    def set_vars(self, vars: Dict[str, Any]):
        if vars is None or not isinstance(vars, dict):
            print("Vars must be a non-empty dict. Will do nothing!")
            return
        if self.is_inprogress:
            self.sql_processor.set_vars(vars)
        self.initial_vars = vars

    def set_spark_configs(self, configs: Dict[str, str]):
        self.sql_processor.set_spark_configs(configs)

    @property
    def templates(self) -> Dict[str, str]:
        return self.sql_processor.templates

    @property
    def tempviews(self) -> List[str]:
        return self.backend.temp_tables()

    def refresh_initial_tempview(self):
        self.initial_temp_views = self.tempviews

    def native_sql(self, sql: str) -> Any:
        return self.backend.exec_native_sql(sql)

    def sql(self, sql: str) -> Any:
        return self.backend.exec_sql(sql)

    def showdf(self, table_name: str):
        self.sql(f"select * from {table_name}").show()

    def step(self, step_no: int) -> Optional[Step]:
        return self.steps[step_no - 1] if 1 <= step_no <= len(self.steps) else None

    def print_steps(self):
        for i in range(len(self.steps)):
            print(f"Step {i + 1}: {str(self.step(i + 1))}")

    def step_on(self):
        if self.step(self._current_step_index + 1 + 1):
            self.sql_processor.run_step(self.steps[self._current_step_index + 1], True)
            self._current_step_index += 1
        else:
            print("Process already ended! Nothing to run!")

    def step_to(self, step_no: int):
        if step_no <= 0 or step_no > len(self.steps):
            print(f"step_index must be from [1...{len(self.steps)}], got {step_no}. Will not run anything!")
            return
        step_index_0_based = step_no - 1
        if step_index_0_based <= self._current_step_index:
            print(f"We are at step {self._current_step_index + 1} now. Nothing to run!")
            return
        while self._current_step_index < step_index_0_based:
            self.step_on()

    def run(self):
        for _ in range(self.left_step_count):
            self.step_on()

    def run_to(self, step_no: int):
        return self.step_to(step_no)

    def restart(self):
        self.backend.clear_cache()
        self.backend.clear_temp_tables(exclude=self.initial_temp_views)
        self.sql_processor = self._create_sql_processor()
        self.steps = self.sql_processor.step_list
        self._current_step_index = -1

    def report(self, verbose: bool = True):
        self.sql_processor.reporter.print_report(verbose=verbose)
