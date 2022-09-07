from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

from ...logger import logger
from .base import Backend, Row, SaveMode, Table, TableMeta

if TYPE_CHECKING:
    from pyflink.common import Row as PyFlinkRow
    from pyflink.table import Table as PyFlinkTable

__all__ = ["FlinkRow", "FlinkTable", "FlinkBackend"]


class FlinkRow(Row):
    def __init__(self, row=None, fields: Optional[List[str]] = None):
        assert row is not None
        self.row: PyFlinkRow = row
        if fields is not None:
            self.row._fields = fields

    def as_dict(self):
        return self.row.as_dict()

    def as_tuple(self) -> Tuple:
        return self.row  # type: ignore

    def __eq__(self, other):
        return self.row.__eq__(other.row)

    def __str__(self):
        return str(self.row)[4:-1]

    def __getitem__(self, i):
        return self.row.__getitem__(i)

    def __repr__(self):
        return self.row.__repr__()


class FlinkTable(Table):
    def __init__(self, table):
        self.table: PyFlinkTable = table

    def is_empty(self) -> bool:
        with self.table.limit(1).execute().collect() as result:
            return len(list(result)) == 0

    def field_names(self) -> List[str]:
        return self.table.get_schema().get_field_names()

    def first(self) -> Row:
        fields = self.table.get_schema().get_field_names()
        with self.table.execute().collect() as result:
            collected_result = [FlinkRow(item, fields) for item in result]
        return FlinkRow() if len(collected_result) == 0 else collected_result[0]

    def limit(self, count: int) -> FlinkTable:
        return FlinkTable(self.table.limit(count))

    def with_column(self, name: str, value: Any) -> FlinkTable:
        from pyflink.table.expression import Expression
        from pyflink.table.expressions import lit

        return FlinkTable(self.table.add_columns((value if isinstance(value, Expression) else lit(value)).alias(name)))

    def collect(self) -> List[Row]:
        fields = self.table.get_schema().get_field_names()
        return [FlinkRow(item, fields) for item in self.table.execute().collect()]

    def show(self, count: int = 20):
        self.table.limit(count).execute().print()

    def count(self) -> int:
        with self.table.execute().collect() as result:
            return len(list(result))


class FlinkBackend(Backend):
    # todo: 考虑是否需要在外面实例化flink: TableEnvironment
    def __init__(self, is_batch: Optional[bool] = True):
        from pyflink.table import EnvironmentSettings, TableEnvironment

        self.flink: TableEnvironment = TableEnvironment.create(
            EnvironmentSettings.in_batch_mode() if is_batch else EnvironmentSettings.in_streaming_mode()
        )

    def init_udfs(self, scala_udf_initializer: Optional[str] = None, *args, **kwargs):
        if scala_udf_initializer:
            from py4j.java_gateway import java_import
            from pyflink.java_gateway import get_gateway

            gw = get_gateway()
            java_import(gw.jvm, scala_udf_initializer)
            initUdfs = eval(f"gw.jvm.{scala_udf_initializer}.initUdfs", {"gw": gw})
            initUdfs(self.flink._j_tenv)

    def register_udfs(self, funcs: Dict[str, Callable]):
        from pyflink.table.udf import UserDefinedScalarFunctionWrapper

        for key in funcs:
            func = funcs[key]
            if isinstance(func, UserDefinedScalarFunctionWrapper):
                self.flink.create_temporary_system_function(key, func)

    def clean(self):
        for temp_view in self.flink.list_temporary_views():
            self.flink.drop_temporary_view(temp_view)

    def exec_native_sql(self, sql: str):
        logger.info(f"will exec sql: {sql}")
        return self.flink.execute_sql(sql)

    def exec_sql(self, sql: str) -> Table:
        logger.info(f"will exec sql: {sql}")
        return FlinkTable(self.flink.sql_query(sql))

    def create_empty_table(self):
        return FlinkTable("")

    def create_temp_table(self, table: Table, name: str):
        assert isinstance(table, FlinkTable)
        self.flink.create_temporary_view(name, table.table)

    def create_cache_table(self, table: Table, name: str):
        assert isinstance(table, FlinkTable)
        self.flink.create_temporary_view(name, table.table)

    def table_exists(self, table: TableMeta):
        catalog = table.catalog_name if table.catalog_name else self.flink.get_current_catalog()
        database = table.dbname if table.dbname else self.flink.get_current_database()
        from pyflink.table.catalog import ObjectPath

        return self.flink.get_catalog(catalog).table_exists(ObjectPath(database, table.pure_table_name))

    def save_table(
        self,
        source_table_meta: TableMeta,
        target_table_meta: TableMeta,
        save_mode: SaveMode,
        create_target_table: bool = False,
    ):
        from pyflink.table.expressions import col, lit

        if not self.table_exists(target_table_meta):
            raise Exception(
                f"target table {target_table_meta.table_name} does not exist, "
                f"cannot save table {target_table_meta.table_name} to {target_table_meta.table_name}"
            )

        temp_res = self.flink.sql_query(f"select * from {source_table_meta.table_name}")
        # 纯动态分区时，如果当日没有新增数据，则不会创建 partition。而我们希望对于静态分区，总是应该创建分区，即使当日没有数据
        static_partitions = list(filter(lambda p: p.value, target_table_meta.partitions))
        columns = (
            self.flink.sql_query(f"select * from {target_table_meta.table_name}")
            .limit(0)
            .get_schema()
            .get_field_names()
        )
        for p in static_partitions:
            temp_res = temp_res.add_columns(lit(p.value).alias(p.field))
        temp_res = temp_res.select(*[col(column) for column in columns])

        temp_res.execute_insert(target_table_meta.table_name, save_mode == SaveMode.overwrite)

    def refresh_table_partitions(self, table: TableMeta):
        # flink无法从`desc table`中解析出partition字段，但是可以在flink_source_file中配置table的partition字段
        pass

    def _register_catalog(self, flink_config):
        if "excution" in flink_config and "catalog" in flink_config["excution"]:
            catalog = flink_config["excution"]["catalog"]
            catalog_name = catalog["name"]
            del catalog["name"]
            catalog_expr = " , ".join([f"'{option}' = '{catalog[option]}'" for option in catalog])
            try:  # noqa: SIM105
                self.exec_native_sql(
                    f"""
                        CREATE CATALOG {catalog_name}
                        WITH (
                            {catalog_expr}
                        );
                    """
                )
            except Exception:
                logger.warn(f"create hive catalog {catalog_name} failed.")

    def _register_tables(self, flink_config, tables: List[str]):
        if len(tables) == 0:
            return
        for table in tables:
            db_name, table_config, connector = self.get_table_config_and_connector(flink_config, table)
            if db_name and table_config and connector:
                self.exec_native_sql(f"create database if not exists {db_name}")
                self._create_table(table, table_config, connector)

    def get_table_config_and_connector(self, flink_config, table: str):
        db_name = table.strip().split(".")[0]
        database = next(filter(lambda t: t["name"] == db_name, flink_config["databases"]), None)
        if not database:
            logger.warn(
                f"database {db_name} does not exist in flink tables config file, register table {table} failed."
            )
            return None, None, None

        table_config = next(filter(lambda t: t["name"] == table.strip().split(".")[1], database["tables"]), None)
        if not table_config:
            logger.warn(f"table {table} does not exist in flink tables config file, register table {table} failed.")
            return None, None, None

        connectors = database["connectors"]
        connector_name = table_config["connector"]["name"]
        connector = next(filter(lambda conn: conn["name"] == connector_name, connectors), None)
        if not connector:
            logger.warn(
                f"connector {connector_name} does not exist in flink tables config file, register table {table} failed."
            )
            return None, None, None
        return db_name, table_config, connector

    def _create_table(self, table: str, table_config, connector):
        schema = table_config["schema"]
        schema_expr = " , ".join(schema)
        partition_by_expr = (
            f"""
                PARTITIONED BY ({','.join(table_config['partition_by'])})"""
            if "partition_by" in table_config
            else ""
        )
        options = connector["options"]
        options.update(table_config["connector"]["options"])
        options_expr = " , ".join([f"'{option}' = '{options[option]}'" for option in options])
        create_sql = f"""
            create table if not exists {table.strip()} (
                {schema_expr}
            )
            {partition_by_expr}
            WITH (
                {options_expr}
            );
        """
        self.exec_native_sql(create_sql)

    def register_tables(self, flink_tables_file_path: str, tables: List[str]):
        if flink_tables_file_path and os.path.exists(flink_tables_file_path):
            with open(flink_tables_file_path, "r") as f:
                config = json.loads(f.read())
                self._register_catalog(config)
                self._register_tables(config, tables)

    def add_jars(self, jars_path: List[str]):
        self.flink.get_config().set("pipeline.jars", f'{";".join([f"file://{path}" for path in jars_path])}')
