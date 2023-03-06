from __future__ import annotations

import os
import traceback
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, List, Optional, Union

from ..logger import logger
from . import SqlProcessorException

if TYPE_CHECKING:
    import pandas as pd
    from sqlalchemy.engine import Connection

    from .backend import Backend
    from .backend.rdb import RdbBackend
    from .context import ProcessorContext
    from .step import Step


__all__ = ["ColumnFuncs", "TableFuncs", "PartitionFuncs", "AlertFunc", "AnalyticsFuncs"]


class ColumnFuncs:
    def __init__(self, backend: Backend):
        self.backend = backend

    def all_cols_without_one_expr(self, table_name: str, *cols_to_exclude: str) -> str:
        return self.all_cols_with_exclusion_expr(table_name, *cols_to_exclude)

    def all_cols_with_exclusion_expr(self, table_name: str, *cols_to_exclude: str) -> str:
        pure_table_name = table_name.split(".")[1] if "." in table_name else table_name
        fields = self.backend.exec_sql(f"select * from {table_name} limit 0").field_names()
        return ", ".join(
            [
                f"{pure_table_name}.{col}"
                for col in fields
                if col not in cols_to_exclude or (col.find(".") != -1 and col.split(".")[-1] not in cols_to_exclude)
            ]
        )

    def all_cols_prefixed_with_exclusion_expr(self, table_name: str, prefix: str, *cols_to_exclude: str) -> str:
        # this function not support pg yet
        pure_table_name = table_name.split(".")[1] if "." in table_name else table_name
        fields = self.backend.exec_sql(f"select * from {table_name} limit 0").field_names()
        return ", ".join(
            [
                f"{pure_table_name}.{col} as `{prefix}{col}`"
                for col in fields
                if col not in cols_to_exclude or (col.find(".") != -1 and col.split(".")[-1] not in cols_to_exclude)
            ]
        )


class TableFuncs:
    def __init__(self, backend: Backend):
        self.backend = backend

    def ensure_no_null_data_in_table(self, step: Step, table_name: str, query: Optional[str] = None) -> bool:
        fields = self.backend.exec_sql(f"select * from {table_name} limit 0").field_names()
        return self._check_not_null_columns_in_table(step, table_name, fields, query, "ensure_no_null_data_in_table")

    def check_not_null_column_in_table(
        self, step: Step, table_name: str, not_null_column: str, query: Optional[str] = None
    ) -> bool:
        return self._check_not_null_columns_in_table(
            step, table_name, [not_null_column], query, "check_not_null_column_in_table"
        )

    def _check_not_null_columns_in_table(
        self,
        step: Step,
        table_name: str,
        not_null_columns: List[str],
        query: Optional[str] = None,
        context: str = "check_not_null_column_in_table",
    ) -> bool:
        null_counts = {}
        for field in not_null_columns:
            condition_sql = f"{field} is null"
            sql = f"select count(1) from {table_name} where " + (
                condition_sql if query is None else f"({condition_sql}) and ({query})"
            )
            null_count = self.backend.exec_sql(sql).collect()[0][0]
            null_counts[field] = null_count
        if sum(null_counts.values()) != 0:
            null_count_msg = "\n".join([f"{v} null rows({k})" for k, v in null_counts.items() if v != 0])
            msg = f"{context} {table_name} failed, found: \n{null_count_msg}"
            logger.info(msg)
            step.collect_report(message=msg)
            return False
        else:
            return True


class PartitionFuncs:
    def __init__(self, backend: Backend):
        self.backend = backend

    def is_not_first_partition(self, table_name: str, partition_value: str) -> bool:
        return not self.is_first_partition(table_name, partition_value)

    def is_first_partition(self, table_name: str, partition_value: str) -> bool:
        partition_values = self._get_partition_values(table_name)
        return partition_value == str(partition_values[0]) if len(partition_values) > 0 else False

    def _get_partition_values(self, table_name) -> List[Union[str, int]]:
        raise NotImplementedError()

    def _get_partition_values_as_str(self, table_name) -> List[str]:
        return [str(v) for v in self._get_partition_values(table_name)]

    def partition_exists(self, table_name: str, partition_value: str) -> bool:
        return partition_value in self._get_partition_values_as_str(table_name)

    def get_partition_values_as_joined_str(self, table_name: str) -> str:
        return ", ".join([f"'{v}'" for v in self._get_partition_values(table_name)])

    def ensure_table_partition_exists(self, step: Step, partition_value: str, table: str, *tables: str) -> bool:
        tables = (table,) + tables
        partition_not_exists_tables = []
        for table in tables:
            try:
                if not self.partition_exists(table, partition_value):
                    partition_not_exists_tables.append(table)
            except Exception:
                partition_not_exists_tables.append(table)
        if len(partition_not_exists_tables) > 0:
            message = f"partition {partition_value} not exists: {partition_not_exists_tables}"
            logger.info(message)
            step.collect_report(message=message)
            return False
        return True

    def ensure_partition_exists(self, step: Step, *args) -> bool:
        if len(args) < 2:
            raise Exception(
                "must contains at least one table and exactly one partition_value when calling"
                f" ensure_partition_exists, got {args}"
            )
        partition_value = args[-1]
        tables = args[:-1]
        return self.ensure_table_partition_exists(step, partition_value, tables[0], *tables[1:])

    def ensure_dwd_partition_exists(self, step: Step, table_name: str, partition_value: str, *foreign_key_cols) -> bool:
        check_ok = True
        try:
            if not self.partition_exists(table_name, partition_value):
                first_partition = self.get_first_partition_optional(table_name)
                if not first_partition or partition_value > first_partition:
                    message = f"partition {partition_value} not exists: {table_name}"
                    logger.info(message)
                    step.collect_report(message=message)
                    check_ok = False
        except Exception:
            message = f"partition {partition_value} not exists: {table_name}"
            logger.info(message)
            step.collect_report(message=message)
            check_ok = False

        if len(foreign_key_cols) > 0:
            partition_values = self._get_partition_values(table_name)
            if len(partition_values) > 0:  # this indicates first partition exists
                partition_col, first_partition = self.get_partition_col(table_name), str(partition_values[0])
                partition_value_to_use = partition_value if partition_value > first_partition else first_partition
                partition_value_to_use_expr = (
                    f"'{partition_value_to_use}'" if isinstance(partition_values[0], str) else partition_value_to_use
                )
                sql = (
                    f"select count(1) as total_count from {table_name} where"
                    f" {partition_col}={partition_value_to_use_expr}"
                )
                total_count = self.backend.exec_sql(sql).collect()[0][0]
                if total_count > 0:
                    fk_col_non_null_expr = " or ".join([f"{fk_col} is not null" for fk_col in foreign_key_cols])
                    sql = (
                        f"select 1 as nonnull_count from {table_name} "
                        f"where ({fk_col_non_null_expr}) and {partition_col}={partition_value_to_use_expr} limit 1"
                    )
                    any_fk_col_non_null_rows = self.backend.exec_sql(sql).collect()
                    if len(any_fk_col_non_null_rows) == 0:
                        message = (
                            f"all fk cols are null in partition: table_name={table_name},"
                            f" partition={partition_value_to_use}"
                        )
                        logger.info(message)
                        step.collect_report(message=message)
                        check_ok = False

        return check_ok

    def ensure_table_partition_or_first_partition_exists(
        self, step: Step, partition_value: str, table: str, *tables: str
    ) -> bool:
        tables = (table,) + tables
        partition_not_exists_tables = []
        for table in tables:
            try:
                if not self.partition_exists(table, partition_value):
                    first_partition = self.get_first_partition_optional(table)
                    if not first_partition or partition_value > first_partition:
                        partition_not_exists_tables.append(table)
            except Exception:
                partition_not_exists_tables.append(table)
        if len(partition_not_exists_tables) > 0:
            message = f"partition {partition_value} not exists: {partition_not_exists_tables}"
            logger.info(message)
            step.collect_report(message=message)
            return False
        return True

    def ensure_partition_or_first_partition_exists(self, step: Step, *args) -> bool:
        if len(args) < 2:
            raise Exception(
                "must contains at least one table and exactly one partition_value when calling"
                f" ensure_partition_exists, got {args}"
            )
        partition_value = args[-1]
        tables = args[:-1]
        return self.ensure_table_partition_or_first_partition_exists(step, partition_value, tables[0], *tables[1:])

    def partition_not_exists(self, table_name: str, partition_value: str) -> bool:
        return not self.partition_exists(table_name, partition_value)

    def previous_partition_exists(self, table_name: str, curr_partition_value_as_dt: str) -> bool:
        partition_values = self._get_partition_values_as_str(table_name)
        partition_date_format = "%Y-%m-%d" if "-" in curr_partition_value_as_dt else "%Y%m%d"
        try:
            curr_partition_value_dt = datetime.strptime(curr_partition_value_as_dt, partition_date_format)
        except ValueError:
            raise SqlProcessorException(
                f"partition value must be date of format `%Y-%m-%d` or `%Y%m%d`, found {curr_partition_value_as_dt}"
            )
        previous_partition_value = (curr_partition_value_dt - timedelta(days=1)).strftime(partition_date_format)
        return previous_partition_value in partition_values

    def get_partition_or_first_partition(self, table_name: str, partition_value: str):
        partition_values = self._get_partition_values_as_str(table_name)
        if len(partition_values) == 0:
            return partition_value
        if partition_value in partition_values:
            return partition_value
        return partition_values[0] if partition_value < partition_values[0] else partition_value

    def get_first_partition_optional(self, table_name: str) -> Optional[str]:
        partition_values = self._get_partition_values(table_name)
        return str(partition_values[0]) if len(partition_values) > 0 else None

    def get_first_partition(self, table_name: str) -> str:
        first_partition = self.get_first_partition_optional(table_name)
        if not first_partition:
            raise Exception(f"no partition found for table {table_name}")
        return first_partition

    def get_last_partition(self, table_name: str) -> str:
        partition_values = self._get_partition_values(table_name)
        last_partition = partition_values[-1] if len(partition_values) > 0 else None
        if not last_partition:
            raise Exception(f"no partition found for table {table_name}")
        return str(last_partition)

    def get_partition_cols(self, table_name: str) -> List[str]:
        raise NotImplementedError()

    def get_partition_col(self, table_name: str):
        partition_cols = self.get_partition_cols(table_name=table_name)
        if not partition_cols:
            raise Exception(f"no partition columns found for table {table_name}")
        return partition_cols[0]

    def has_partition_col(self, table_name: str):
        return len(self.get_partition_cols(table_name)) > 0


class Alerter:
    def send_alert(self, msg: str, mentioned_users: str = ""):
        raise NotImplementedError()


class AlertFunc:
    def __init__(self, backend: Optional[Backend], alerter: Alerter):
        self.backend = backend
        self.alerter = alerter

    def alert_with_backend(
        self,
        backend: Backend,
        step: Step,
        context: ProcessorContext,
        rule_name: str,
        pass_condition: str,
        alert_template: str,
        mentioned_users: str,
    ):
        # Fetch 10 rows at most
        assert step.select_sql is not None
        alert_data = backend.exec_sql(step.select_sql).limit(10).collect()

        msg_list = []
        for data in alert_data:
            check_result = data.as_dict()
            context.add_vars(check_result)
            if not step.func_runner.run_func(pass_condition.format(**check_result), context.vars_context):
                msg_list.append(alert_template.format(**check_result))
        if msg_list:
            self.alerter.send_alert("\n".join(msg_list), mentioned_users)

    def alert(
        self,
        step: Step,
        context: ProcessorContext,
        rule_name: str,
        pass_condition: str,
        alert_template: str,
        mentioned_users: str,
    ):
        assert self.backend is not None
        self.alert_with_backend(self.backend, step, context, rule_name, pass_condition, alert_template, mentioned_users)

    def alert_exception_handler(self, rule_name: str, mentioned_users: str):
        def exception_handler(e):
            logger.error(f"{traceback.format_exc()}")
            self.alerter.send_alert(f"执行{rule_name}失败: {e}", mentioned_users)

        return exception_handler


class IOFuncs:
    def move_file(self, source_file: str, target_file: str):
        if not os.path.exists(source_file):
            raise FileNotFoundError(source_file)
        os.makedirs(os.path.dirname(target_file), exist_ok=True)
        if os.path.exists(target_file):
            os.remove(target_file)
        try:
            os.rename(source_file, target_file)
        except OSError:
            with open(target_file, "w") as fw, open(source_file, "r") as fr:
                fw.write(fr.read())
            os.remove(source_file)
        logger.info(f"file moved: {source_file} -> {target_file}")


class AnalyticsFuncs:
    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    def data_profiling_report(
        self,
        table: str,
        query: str,
        output_folder: str,
        max_count: str = "50000",
        include_correlations: str = "true",
        types: Union[str, List[str]] = "html",
    ):
        try:
            from ydata_profiling import ProfileReport
        except ModuleNotFoundError:
            from pandas_profiling import ProfileReport

        from easy_sql.sql_processor.backend.rdb import RdbBackend

        if isinstance(types, str):
            types = [t.strip() for t in types.split(",") if t.strip()]
        for type in types:
            if type not in ["html", "json"]:
                raise Exception(f"Found unknown type {type}, all supported are: html/json")

        _max_count = int(max_count)

        backend = self.backend
        if backend.is_rdb_backend:
            if backend.is_bigquery_backend:
                raise Exception(f"Not supported backend: {backend.__class__}")
            assert isinstance(backend, RdbBackend)
            df = self._read_data_rdb(backend, table, query, _max_count)
        elif backend.is_spark_backend:
            df = self._read_data_spark(backend, table, query, _max_count)
        else:
            raise Exception(f"Not supported backend: {backend.__class__}")

        if df is None:
            return

        _include_correlations = include_correlations.lower() in ["1", "true", "y", "yes"]
        if _include_correlations:
            profile = ProfileReport(df, title=f"Profiling Report for {table}")
        else:
            logger.info("profiling with no correlations...")
            profile = ProfileReport(
                df,
                title=f"Profiling Report for {table}",
                correlations=None,
                vars={"num": {"chi_squared_threshold": False}, "cat": {"chi_squared_threshold": False}},
                interactions={"targets": [], "continuous": False},
            )

        if "." in table:
            db, table = tuple(table.split("."))
            profiling_file = f"{output_folder}/{db}/{table}.html"
        else:
            profiling_file = f"{output_folder}/{table}.html"
        os.makedirs(os.path.dirname(profiling_file), exist_ok=True)

        if "html" in types:
            profile.to_file(profiling_file)
            logger.info(f"generated file: {profiling_file}")
        if "json" in types:
            profiling_file = profiling_file[: profiling_file.rindex(".")] + ".json"
            with open(profiling_file, "w") as f:
                f.write(profile.to_json())
            logger.info(f"generated file: {profiling_file}")

    def _read_data_rdb(self, backend: RdbBackend, table: str, query: str, max_count: int) -> pd.DataFrame:
        import pandas as pd
        from sqlalchemy.sql import text

        rand_func = "rand" if backend.is_clickhouse_backend else "random"
        condition_sql = "where " + query if query else ""
        sql = f"select * from {table} {condition_sql} order by {rand_func}() limit {max_count}"
        with backend.engine.connect().execution_options(autocommit=True) as conn:
            conn: Connection = conn
            query_result = conn.execute(text(sql))
        return pd.DataFrame(query_result.fetchall())

    def _read_data_spark(self, backend: Backend, table: str, query: str, max_count: int) -> Optional[pd.DataFrame]:
        from pyspark.sql.functions import expr
        from pyspark.sql.types import ArrayType, DecimalType, MapType

        condition_sql = "where " + query if query else ""
        sql = f"select count(*) from {table} {condition_sql}"
        count = backend.exec_native_sql(sql).collect()[0][0]

        sample_fraction = 1.0 if count < max_count else float(max_count) / float(count)

        spark_df = backend.exec_native_sql(f"select * from {table} {condition_sql}").sample(fraction=sample_fraction)
        spark_df.cache()

        if spark_df.count() == 0:
            # if we don't handle this case pandas profiling will raise error
            logger.info(f"{table} is empty, no report generated")
            return None

        for field in spark_df.schema.fields:
            if isinstance(field.dataType, DecimalType):
                # cast decimal to double to avoid pandas object type after converting later on
                spark_df = spark_df.withColumn(field.name, expr(f"cast(`{field.name}` as double)"))
            if isinstance(field.dataType, (ArrayType, MapType)):
                spark_df = spark_df.withColumn(field.name + "__size", expr(f"size(`{field.name}`)"))

        df = spark_df.toPandas()
        return df


class TestFuncs:
    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    def sleep(self, secs: str):
        try:
            _secs = float(secs)
        except ValueError:
            raise Exception(f"secs must be an float when sleep, got `{secs}`")
        import time

        time.sleep(_secs)
