from __future__ import annotations

import re
import uuid
from os import path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from easy_sql.sql_processor.backend.rdb import RdbBackend
from easy_sql.utils.sql_expr import CommentSubstitutor, remove_semicolon_from_line

from ..logger import logger
from .backend import Backend, Partition, SaveMode
from .backend import Table as BackendTable
from .backend import TableMeta as Table
from .common import SqlProcessorException

if TYPE_CHECKING:
    from .context import ProcessorContext
    from .funcs import FuncRunner

__all__ = ["StepConfig", "Step", "StepType", "ReportCollector", "StepFactory", "ExecutedSqlTransformer", "SqlCleaner"]


class ReportCollector:
    def collect_report(self, step: Step, status: Optional[str] = None, message: Optional[str] = None):
        raise NotImplementedError()

    def collected_report(self, step: Step) -> Any | None:
        raise NotImplementedError()


class EmptyReportCollector(ReportCollector):
    def collect_report(self, step: Step, status: Optional[str] = None, message: Optional[str] = None):
        logger.info(f"collect report for step: {str(step)}, status: {status}, message: {message}")

    def collected_report(self, step: Step) -> Any | None:
        return None


class SqlCleaner:
    def __init__(self) -> None:
        self.comment_substituter = CommentSubstitutor()

    def clean_sql(self, sql: str) -> str:
        """
        Clean a sql with comments and semicolon to form a clean select sql, in order to be used to construct a new sql.
        It works as below:
        - remove leading comments
        - remove tail comments
        - remove the last semicolon
        """
        sql_lines = sql.split("\n")
        for i, sql_line in enumerate(sql_lines):
            sql_line = sql_line.strip()
            if sql_line and not sql_line.startswith("--"):
                sql_lines = sql_lines[i:]
                sql_lines[0] = sql_line
                break
        sql_lines.reverse()
        for i, sql_line in enumerate(sql_lines):
            sql_line = sql_line.strip()
            if sql_line and not sql_line.startswith("--") and not sql_line.startswith(";"):
                sql_lines = sql_lines[i:]
                sql_lines[0] = sql_line
                break
        sql_lines.reverse()
        sql_lines[-1] = self.comment_substituter.remove(sql_lines[-1]).rstrip().rstrip(";")
        clean_sql = "\n".join(sql_lines)
        return clean_sql


class StepConfig:
    STEP_CONFIG_PATTERN = r"^-- target\s*=\s*(\w+)(.*)$"

    def __init__(
        self,
        step_type: str,
        step_name: Optional[str],
        condition: Optional[str],
        line_no: int,
        step_config_str: str = "",
    ):
        self.step_type = step_type
        self.name = step_name
        self.condition = condition
        self.line_no = line_no
        self.step_config_str = step_config_str
        self.rendered_condition = condition
        self.rendered_name = step_name

    def update_rendered_condition(self, rendered_condition: str):
        self.rendered_condition = rendered_condition

    def update_rendered_name(self, rendered_name: str):
        self.rendered_name = rendered_name

    def __str__(self):
        return (
            f"StepConfig(target={self.step_type}.{self.rendered_name}, condition={self.rendered_condition},"
            f" line_no={self.line_no})"
        )

    def __repr__(self):
        return str(self)

    def __eq__(self, other: StepConfig):
        return (
            isinstance(other, StepConfig)
            and self.step_type == other.step_type
            and self.name == other.name
            and self.condition == other.condition
            and self.line_no == other.line_no
        )

    @staticmethod
    def from_config_line(config_line: str, line_no: int) -> StepConfig:
        configs = re.compile(r"^\s*-- ").sub("", config_line).strip()
        configs = configs[configs.index("=") + 1 :]
        target_type = configs[: configs.index(".")] if configs.find(".") != -1 else configs
        target_type = target_type[: target_type.index(",")] if target_type.find(",") != -1 else target_type
        if target_type not in StepType.all():
            raise SqlProcessorException(f"unknown step type: type={target_type}, supported_types={StepType.all()}")

        target_name = configs[configs.index(".") + 1 :] if configs.find(".") != -1 else None
        target_name = target_name.strip() if target_name is not None and target_name.strip() != "" else None
        target_condition = None

        if target_name is not None:
            condition_match = re.compile(r"^\s*(.*)\s*,\s*if\s*=(.*$)").match(target_name)
        else:
            condition_match = re.compile(r"^\s*(,)\s*if\s*=(.*$)").match(configs[len(target_type) :])
        if condition_match:
            target_name = condition_match.group(1) if target_name is not None else None
            target_condition = condition_match.group(2).strip()
            reg_exp = r"[a-zA-Z0-9_]*\([^()]*\)"
            if not re.compile(reg_exp).match(target_condition):
                raise SqlProcessorException(
                    f"parse step config failed. condition must be like {reg_exp}, but got {target_condition}."
                    f" config_line={config_line}"
                )

        return StepConfig(
            step_name=target_name,
            step_type=target_type,
            condition=target_condition,
            line_no=line_no,
            step_config_str=config_line,
        )

    def is_target_name_a_func(self):
        return "(" in self.name if self.name is not None else False

    def has_condition(self):
        return self.condition is not None


class StepType:
    TEMP = "temp"
    CACHE = "cache"
    BROADCAST = "broadcast"
    TEMPLATE = "template"
    FUNC = "func"
    LOG = "log"
    CHECK = "check"
    VARIABLES = "variables"
    LIST_VARIABLES = "list_variables"
    HIVE = "hive"
    OUTPUT = "output"
    ACTION = "action"

    @staticmethod
    def all() -> List[str]:
        return [
            StepType.TEMP,
            StepType.CACHE,
            StepType.TEMPLATE,
            StepType.LOG,
            StepType.CHECK,
            StepType.VARIABLES,
            StepType.HIVE,
            StepType.FUNC,
            StepType.BROADCAST,
            StepType.OUTPUT,
            StepType.LIST_VARIABLES,
            StepType.ACTION,
        ]


class ExecutedSqlTransformer:
    def transform_create_view_sql(self, view_name: str, select_sql: str):
        raise NotImplementedError()

    def transform_func_ref_sql(self, func_ref_sql: Optional[str]):
        return func_ref_sql

    def transform_save_table_sql(self, table_name: str, select_sql: str):
        raise NotImplementedError()


class SimpleExecutedSqlTransformer(ExecutedSqlTransformer):
    def __init__(self) -> None:
        self._sql_cleaner = SqlCleaner()
        super().__init__()

    def cleaned_select_sql(self, sql: str) -> Optional[str]:
        return self._sql_cleaner.clean_sql(sql)

    def transform_create_view_sql(self, view_name: str, select_sql: str):
        clean_sql = self._sql_cleaner.clean_sql(select_sql)
        assert clean_sql is not None
        if re.match(r"^with\s", clean_sql, re.I):
            return f"create view {view_name} as (\n{clean_sql}\n);"
        return f"insert overwrite {view_name} \n{clean_sql}\n;"

    def transform_save_table_sql(self, table_name: str, select_sql: str):
        clean_sql = self._sql_cleaner.clean_sql(select_sql)
        assert clean_sql is not None
        if re.match(r"^with\s", clean_sql, re.I):
            return f"insert overwrite {table_name} (\n{clean_sql}\n);"
        return f"insert overwrite {table_name} \n{clean_sql}\n;"


class OutputTableNamer:

    def output_table_name(self, table_name: str) -> str:
        return table_name


class Step:
    def __init__(
        self,
        id: str,
        reporter_collector: ReportCollector,
        func_runner: FuncRunner,
        target_config: Optional[StepConfig] = None,
        select_sql: Optional[str] = None,
        debug_var_tmpl_replace: bool = False,
        *,
        executed_sql_transformer: Optional[ExecutedSqlTransformer] = None,
        output_table_namer: Optional[OutputTableNamer] = None,
    ):
        self.id = id
        self.target_config = target_config
        self.select_sql = select_sql
        self.debug_var_tmpl_replace = debug_var_tmpl_replace
        self.reporter_collector = reporter_collector
        self.func_runner = func_runner
        self.executed_sql: Optional[str] = None
        self.executed_sql_transformer = executed_sql_transformer or SimpleExecutedSqlTransformer()
        self.output_table_namer = output_table_namer or OutputTableNamer()

    def __str__(self):
        return str(self.target_config).replace("StepConfig(", "Step(", 1)

    def __repr__(self):
        return f"[\n    config: {self.target_config},\n    sql: {self.select_sql}\n]"

    def should_run(self, context: ProcessorContext):
        assert self.target_config is not None
        variables: dict = context.vars_context.vars
        if "__skip_all__" in variables and variables["__skip_all__"] in ["TRUE", True, 1, "True", "true", "1"]:
            return False
        if not self.target_config.has_condition():
            return True
        assert self.target_config.condition is not None
        self.target_config.update_rendered_condition(
            self.func_runner.render_func_call(self.target_config.condition, context.vars_context)
        )
        return self.func_runner.run_func(self.target_config.condition, context.vars_context)

    def read(self, backend: Backend, context: ProcessorContext) -> Optional[BackendTable]:
        assert self.target_config is not None
        if self.target_config.step_type in [StepType.TEMPLATE] or (
            self.target_config.step_type == StepType.CHECK and self._should_skip_check(context.vars_context.vars)
        ):
            return backend.create_empty_table()
        if self.target_config.is_target_name_a_func():
            if self.select_sql:
                self.preprocess_select_sql(context)
            return backend.create_empty_table()
        assert self.select_sql is not None
        self.preprocess_select_sql(context)
        if self.target_config.step_type == StepType.ACTION:
            backend.exec_native_sql(self.select_sql)
            return None
        else:
            return backend.exec_sql(self.select_sql)

    def preprocess_select_sql(self, context: ProcessorContext):
        assert self.select_sql is not None, f"There must be a sql expression for step: {self}"
        self.select_sql = context.replace_templates(self.select_sql)
        self.select_sql = context.replace_variables(self.select_sql)

    def get_executed_sql(self) -> str:
        return self.executed_sql or ""

    def _create_view_sql(self) -> str:
        assert self.target_config is not None
        assert self.target_config.name is not None
        assert self.select_sql is not None
        return self.executed_sql_transformer.transform_create_view_sql(self.target_config.name, self.select_sql)

    def write(self, backend: Backend, table: Optional[BackendTable], context: ProcessorContext, dry_run: bool = False):
        assert self.target_config is not None
        variables: dict = context.vars_context.vars

        if not table:
            return

        if StepType.VARIABLES == self.target_config.step_type and not table.is_empty():
            field_names = table.field_names()
            row = table.first()
            for field_name in field_names:
                index = field_names.index(field_name)
                field_value = None
                if row[index] is not None:
                    field_value = str(row[index])
                context.add_vars({field_name: field_value})

        if StepType.LIST_VARIABLES == self.target_config.step_type:
            field_names = table.field_names()
            rows = table.collect()
            list_vars = {}
            for field_name in field_names:
                index = field_names.index(field_name)
                list_vars[field_name] = [row[index] for row in rows]
            context.add_list_vars(list_vars)

        if StepType.TEMPLATE == self.target_config.step_type:
            assert self.select_sql is not None
            assert self.target_config.name is not None
            context.add_templates({self.target_config.name: self.select_sql})

        elif StepType.TEMP == self.target_config.step_type:
            assert self.target_config.name is not None
            backend.create_temp_table(table, self.target_config.name)
            self.executed_sql = self._create_view_sql()

        elif StepType.CACHE == self.target_config.step_type:
            assert self.target_config.name is not None
            if "__no_cache__" in variables and str(variables["__no_cache__"]).lower() in ["true", "1"]:
                backend.create_temp_table(table, self.target_config.name)
            else:
                backend.create_cache_table(table, self.target_config.name)
            self.executed_sql = self._create_view_sql()

        elif StepType.BROADCAST == self.target_config.step_type:
            assert self.target_config.name is not None
            backend.broadcast_table(table, self.target_config.name)
            self.executed_sql = self._create_view_sql()

        elif StepType.LOG == self.target_config.step_type:
            if "__no_log__" in variables and str(variables["__no_log__"]).lower() in ["true", "1"]:
                return
            self._write_for_log_step(table)

        elif StepType.FUNC == self.target_config.step_type:
            assert self.target_config.name is not None
            self.target_config.update_rendered_name(
                self.func_runner.render_func_call(self.target_config.name, context.vars_context)
            )
            self.executed_sql = self.func_runner.run_func(self.target_config.name, context.vars_context)
            self.executed_sql = self.executed_sql_transformer.transform_func_ref_sql(self.executed_sql)

        elif StepType.CHECK == self.target_config.step_type:
            if self._should_skip_check(variables):
                return
            self._write_for_check_step(table, context)

        elif self.target_config.step_type in [StepType.HIVE, StepType.OUTPUT]:
            self._write_for_output_step(backend, table, context, dry_run)

    def _should_skip_check(self, variables):
        return "__no_check__" in variables and str(variables["__no_check__"]).lower() in ["true", "1"]

    def is_template_statement(self):
        assert self.target_config is not None
        return StepType.TEMPLATE == self.target_config.step_type

    def _write_for_output_step(self, backend: Backend, table: BackendTable, context: ProcessorContext, dry_run: bool):
        assert self.target_config is not None
        assert self.target_config.name is not None
        extra_cols, variables = context.extra_cols, context.vars
        if "." not in self.target_config.name:
            message = (
                "table name for hive or output must be a full name, it should be of format DB.TABLE_NAME, got"
                f" `{self.target_config.name}`"
            )
            self.collect_report(message=message)
            raise SqlProcessorException(message)

        temp_table_name = f'{self.target_config.name.split(".")[1]}_{uuid.uuid4().hex}'
        for col in extra_cols:
            table = table.with_column(col.name, col.value)
        backend.create_temp_table(table, temp_table_name)

        source_table = Table(temp_table_name)
        target_table_name = self.output_table_namer.output_table_name(f"{self.target_config.name}")

        static_partition_name, static_partition_value, create_output_table, save_mode = (
            None,
            None,
            False,
            SaveMode.overwrite,
        )
        dry_run_verify_output_schema, dry_run_verify_output_schema_type = False, False
        for name, value in variables.items():
            if "__partition__" in name:
                static_partition_name = name[len("__partition__") :]
                if backend.is_spark_backend:
                    static_partition_value = value
                else:
                    assert isinstance(backend, RdbBackend)
                    static_partition_value = backend.sql_expr.convert_partition_value(static_partition_name, value)
            if name.lower() == "save_mode" or name.lower() == "__save_mode__":
                save_mode = SaveMode[value.lower()]
            true_values = [True, "true", "TRUE", "True", 1, "1"]
            if name.lower() in ["__create_hive_table__", "__create_output_table__"]:
                create_output_table = value in true_values
            if name.lower() == "__dry_run_verify_output_schema__":
                dry_run_verify_output_schema = value in true_values
            if name.lower() == "__dry_run_verify_output_schema_type__":
                dry_run_verify_output_schema_type = value in true_values

        dynamic_partition = False
        if static_partition_name is not None:
            if static_partition_value is None or str(static_partition_value).strip() == "":
                logger.info(
                    f"partition value not exist or is empty, will do as dynamic partition for col {static_partition_name}"
                )
                static_partition_value = None
                dynamic_partition = True
            target_table = Table(
                target_table_name, partitions=[Partition(field=static_partition_name, value=static_partition_value)]
            )
            mode = "dynamic" if dynamic_partition else "static"
            self.collect_report(message=f"save with {mode} partition: {static_partition_name}={static_partition_value}")
        else:
            dynamic_partition = True
            target_table = Table(target_table_name)
            if not dry_run and backend.table_exists(target_table):
                backend.refresh_table_partitions(target_table)
            self.collect_report(message="save with dynamic partitions")

        if dry_run:
            if not dynamic_partition and static_partition_name:
                if backend.is_spark_backend:
                    from pyspark.sql.functions import lit

                    table = table.with_column(static_partition_name, lit(static_partition_value))
                else:
                    assert isinstance(backend, RdbBackend)
                    partition_value = backend.sql_expr.for_value(static_partition_value)  # type: ignore
                    table = table.with_column(static_partition_name, partition_value)
            backend.create_temp_table(table, temp_table_name + "_output")  # type: ignore
            if dry_run_verify_output_schema:
                backend.verify_schema(
                    Table(temp_table_name + "_output"), target_table, verify_type=dry_run_verify_output_schema_type
                )
            self.collect_report(message="will not save data to data warehouse, since we are in dry run mode")
            # may need to provide a more accurate sql
            assert self.select_sql is not None
            self.executed_sql = self.executed_sql_transformer.transform_save_table_sql(
                target_table.table_name, self.select_sql
            )
            return

        target_table_exists = backend.table_exists(target_table)
        if not target_table_exists and not create_output_table:
            message = f"target table {target_table.table_name} not exists"
            self.collect_report(message=message)
            raise Exception(message)

        backend.save_table(source_table, target_table, save_mode, create_target_table=create_output_table)
        # may need to provide a more accurate sql
        assert self.select_sql is not None
        self.executed_sql = self.executed_sql_transformer.transform_save_table_sql(
            target_table.table_name, self.select_sql
        )

    def _write_for_log_step(self, df: BackendTable):
        assert self.target_config is not None
        assert self.target_config.name is not None
        log_data = df.limit(20).collect()
        if len(log_data) == 0:
            logger.info(f"log for [{self.target_config.name}]: no data to show")
            self.collect_report(message="no data to show")
        elif len(log_data) == 1:
            logger.info(f"log for [{self.target_config.name}]: {str(log_data[0])}")
            self.collect_report(message=f"{str(log_data[0])}")
        else:
            logger.info(f"log for [{self.target_config.name}]: ")
            df.show(20)
            self.collect_report(message="\n".join([str(row) for row in log_data]))

    def _write_for_check_step(self, df: BackendTable, context: ProcessorContext):
        assert self.target_config is not None
        if self.target_config.is_target_name_a_func():
            assert self.target_config.name is not None
            self.target_config.update_rendered_name(
                self.func_runner.render_func_call(self.target_config.name, context.vars_context)
            )
            if not self.func_runner.run_func(self.target_config.name, context.vars_context):
                message = (
                    f"check failed! check function returned False. check={self.target_config.name}, vars={context.vars}"
                )
                self.collect_report(message=message)
                raise SqlProcessorException(message)
            else:
                return

        check_data = df.limit(100).collect()
        if not check_data:
            message = (
                "Data for check must contains at least one row. Please check your sql. "
                f"check={self.target_config.name}, check_data(limit 100)={check_data}, check_data_count={df.count()}"
            )
            self.collect_report(message=message)
            raise SqlProcessorException(message)
        for check_item in check_data:
            check_data_as_dict = check_item.as_dict()
            if "actual" not in check_data_as_dict or "expected" not in check_data_as_dict:
                message = (
                    "Data for check must contains expected and actual data. Please check your sql. "
                    f"check={self.target_config.name}, check_data(limit 100)={check_data}"
                )
                self.collect_report(message=message)
                raise SqlProcessorException(message)
            if check_data_as_dict["actual"] != check_data_as_dict["expected"]:
                message = (
                    f"check [{self.target_config.name}] failed! actual={check_data_as_dict['actual']},"
                    f" expected={check_data_as_dict['expected']}, check_data(limit 100)={check_data}"
                )
                logger.error(message)
                self.collect_report(message=message)
                raise SqlProcessorException(message)
        logger.info(f"check [{self.target_config.name}] passed! check_data(limit 100)={check_data}")
        self.collect_report(message=f"check_data(limit 100)={check_data}")

    def collect_report(self, status=None, message=None):
        self.reporter_collector.collect_report(self, status=status, message=message)


class IncludeResolver:
    def resolve_include(self, include_file: str) -> str | None:
        """
        Resolve include file to sql string. If could not resolve, return None.
        """
        raise NotImplementedError()


class StepFactory:
    def __init__(
        self,
        reporter: ReportCollector,
        func_runner: FuncRunner,
        executed_sql_transformer: Optional[ExecutedSqlTransformer] = None,
        base_dir: Optional[str] = None,
        skip_duplicate_include: bool = False,
        output_table_namer: Optional[OutputTableNamer] = None,
    ):
        self.reporter = reporter
        self.func_runner = func_runner
        self.executed_sql_transformer = executed_sql_transformer
        self.output_table_namer = output_table_namer
        self.base_dir = base_dir
        self.skip_duplicate_include = skip_duplicate_include
        self.resolved_sql = ""

    def create_from_sql(
        self, sql: str, includes: Dict[str, str] | IncludeResolver | None = None, sql_file: str = ""
    ) -> List[Step]:
        includes = includes or {}
        self.resolved_sql = self._resolve_include(sql, includes, current_file=sql_file)
        lines = self.resolved_sql.split("\n")

        index = 0
        sql_parts = []
        step_list = []
        step = Step(
            f"step-{len(step_list) + 1}",
            self.reporter,
            self.func_runner,
            executed_sql_transformer=self.executed_sql_transformer,
            output_table_namer=self.output_table_namer,
        )
        while index < len(lines):
            line = remove_semicolon_from_line(lines[index])
            line_stripped = line.strip()
            if re.compile(StepConfig.STEP_CONFIG_PATTERN, flags=re.IGNORECASE).match(line_stripped):
                if len(sql_parts) > 0:
                    step.select_sql = "\n".join(sql_parts)
                if step.target_config is not None:
                    step_list.append(step)
                step = Step(
                    f"step-{len(step_list) + 1}",
                    self.reporter,
                    self.func_runner,
                    executed_sql_transformer=self.executed_sql_transformer,
                    output_table_namer=self.output_table_namer,
                )
                sql_parts = []
                target_config = StepConfig.from_config_line(line_stripped, index + 1)
                step.target_config = target_config
                if index == len(lines) - 1:
                    step_list.append(step)
            elif index == len(lines) - 1:
                if "" != line_stripped:
                    sql_parts.append(line)
                if len(sql_parts) > 0:
                    step.select_sql = "\n".join(sql_parts)
                step_list.append(step)
            elif "" != line_stripped:
                sql_parts.append(line)
            index += 1
        return step_list

    def _resolve_include(
        self,
        sql,
        includes: Dict[str, str] | IncludeResolver | None = None,
        resolved_includes: List[str] | None = None,
        current_file: str = "",
    ) -> str:
        resolved_includes = resolved_includes or []
        include_sql_pattern = r"^--\s*include\s*=\s*(.*\.sql)\s*$"
        lines = sql.split("\n")
        resoloved_sqls = []
        for i, line in enumerate(lines):
            line = remove_semicolon_from_line(line)
            line_stripped = line.strip()
            matches = re.match(include_sql_pattern, line_stripped, flags=re.IGNORECASE)
            if matches:
                file = matches.group(1)
                if file in resolved_includes and self.skip_duplicate_include:
                    logger.info(f"skip duplicate include file at line {current_file}:{i}: {file}")
                    continue
                resolved_includes.append(file)
                if isinstance(includes, dict) and file in includes:
                    resoloved_sqls.append(
                        self._resolve_include(
                            includes[file], includes, resolved_includes=resolved_includes, current_file=file
                        )
                    )
                    continue
                if isinstance(includes, IncludeResolver):
                    resolved_sql = includes.resolve_include(file)
                    if resolved_sql is not None:
                        resoloved_sqls.append(
                            self._resolve_include(
                                resolved_sql, includes, resolved_includes=resolved_includes, current_file=file
                            )
                        )
                        continue

                try:
                    import importlib

                    func_mod = importlib.import_module("common.file_reader")
                    read_file_func = func_mod.read_file
                    resoloved_sqls.append(
                        self._resolve_include(
                            read_file_func(file), includes, resolved_includes=resolved_includes, current_file=file
                        )
                    )
                except ModuleNotFoundError:
                    logger.info("failed to import common.file_reader, will try default file reader")
                    resoloved_sqls.append(
                        self._resolve_include(
                            SqlSnippetsReader.read_file(file, self.base_dir),
                            includes,
                            resolved_includes=resolved_includes,
                            current_file=file,
                        )
                    )
            else:
                resoloved_sqls.append(line)
        resolved_sql = "\n".join(resoloved_sqls)
        return resolved_sql


class SqlSnippetsReader:
    @staticmethod
    def read_file(file_name: str, base_path: Optional[str] = None) -> str:
        possible_paths = [file_name]
        if base_path:
            possible_paths.append(path.join(base_path, file_name))

        for p in possible_paths:
            if not path.exists(p):
                logger.info(f"file not found, tried: {p}")
                continue
            logger.info(f"read file at path: {p}")
            with open(p) as f:
                return f.read()

        raise FileNotFoundError(f"file not found: tried_paths={possible_paths}")
