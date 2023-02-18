from __future__ import annotations

import json
import os
import re
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

from ...logger import logger
from .base import Backend, Row, SaveMode, Table, TableMeta

if TYPE_CHECKING:
    from pyflink.common import Row as PyFlinkRow
    from pyflink.table import Table as PyFlinkTable
    from pyflink.table import TableResult

__all__ = ["FlinkRow", "FlinkTable", "FlinkBackend", "FlinkTablesConfig"]


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
    def __init__(self, is_batch: Optional[bool] = True, flink_tables_config: Optional[FlinkTablesConfig] = None):
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.table import EnvironmentSettings, StreamTableEnvironment

        self.flink_tables_config = flink_tables_config or FlinkTablesConfig({})

        self.flink_stream_env = None if is_batch else StreamExecutionEnvironment.get_execution_environment()
        env_settings = EnvironmentSettings.in_batch_mode() if is_batch else EnvironmentSettings.in_streaming_mode()
        self.flink: StreamTableEnvironment = StreamTableEnvironment.create(
            stream_execution_environment=self.flink_stream_env,  # type: ignore
            environment_settings=env_settings,
        )

        self.streaming_insert_stmts = self.flink.create_statement_set() if not is_batch else None
        self.has_streaming_insert_stmts = False

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

    def execute_streaming_inserts(self):
        if self.streaming_insert_stmts is not None:
            if self.has_streaming_insert_stmts:
                self.streaming_insert_stmts.execute()
                logger.info("committed insert statements.")
                self.streaming_insert_stmts = self.flink.create_statement_set()
                self.has_streaming_insert_stmts = False
            else:
                logger.info("no insert statements to commit.")

    def clean(self):
        for temp_view in self.flink.list_temporary_views():
            self.flink.drop_temporary_view(temp_view)
        self.execute_streaming_inserts()

    def exec_native_sql(self, sql: str) -> TableResult:
        logger.info(f"will exec sql: {sql}")
        return self.flink.execute_sql(sql)

    def exec_native_sql_query(self, sql: str) -> PyFlinkTable:
        logger.info(f"will exec sql: {sql}")
        return self.flink.sql_query(sql)

    def exec_sql(self, sql: str) -> Table:
        return FlinkTable(self.exec_native_sql_query(sql))

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

    def save_table_sql(self, source_table: TableMeta, source_table_sql: str, target_table: TableMeta) -> str:
        columns = self.exec_native_sql_query(f"select * from {source_table.table_name}").get_schema().get_field_names()
        return f'insert into {target_table.table_name} select {",".join(columns)} from ({source_table_sql})'

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

        temp_res = self.exec_native_sql_query(f"select * from {source_table_meta.table_name}")
        # 纯动态分区时，如果当日没有新增数据，则不会创建 partition。而我们希望对于静态分区，总是应该创建分区，即使当日没有数据
        static_partitions = list(filter(lambda p: p.value, target_table_meta.partitions))
        table_description = list(self.exec_native_sql(f"desc {target_table_meta.table_name}").collect())
        columns: List[str] = [f[0] for f in table_description]  # type: ignore
        for p in static_partitions:
            temp_res = temp_res.add_columns(lit(p.value).alias(p.field))
        temp_res = temp_res.select(*[col(column) for column in columns])

        logger.info(f"save table {source_table_meta.table_name} to {target_table_meta.table_name}")
        if self.streaming_insert_stmts is not None:
            logger.info("add insert to streaming_insert_stmts.")
            self.streaming_insert_stmts.add_insert(
                target_table_meta.table_name, temp_res, save_mode == SaveMode.overwrite
            )
            self.has_streaming_insert_stmts = True
        else:
            temp_res.execute_insert(target_table_meta.table_name, save_mode == SaveMode.overwrite)

    def refresh_table_partitions(self, table: TableMeta):
        # flink无法从`desc table`中解析出partition字段，但是可以在flink_source_file中配置table的partition字段
        pass

    def _register_catalog(self):
        for catalog in self.flink_tables_config.catalogs:
            catalog = catalog.copy()
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
                logger.warn(f"create catalog {catalog_name} failed.", exc_info=True)

    def _register_tables(self, tables: List[str]):
        if len(tables) == 0:
            return
        for table in tables:
            db_name, table_config, connector = self.get_table_config_and_connector(table)
            if db_name and table_config and connector:
                self.exec_native_sql(f"create database if not exists {db_name}")
                self._create_table(table, table_config, connector)
            else:
                logger.warn(f"register table {table} failed, no configuration found")

    def get_table_config_and_connector(self, table: str) -> Tuple[Optional[str], Optional[Dict], Optional[Dict]]:
        db_name = table.strip().split(".")[0]
        database = self.flink_tables_config.database(db_name)
        if not database:
            logger.warn(f"database {db_name} does not exist in flink tables config file")
            return None, None, None

        table_name = table.strip().split(".")[1]
        table_config = self.flink_tables_config.table(database, table_name)
        if not table_config:
            logger.warn(f"table {table} does not exist in flink tables config file")
            return None, None, None

        connector_name = table_config["connector"]["name"]
        connector = self.flink_tables_config.connector(database, connector_name)
        if not connector:
            logger.warn(f"connector {connector_name} does not exist in flink tables config file")
            return None, None, None
        return db_name, table_config, connector

    def get_db_connector(self, db_name: str, connector_name: str) -> Optional[Dict]:
        database = self.flink_tables_config.database(db_name)
        if not database:
            logger.warn(f"database {db_name} does not exist in flink tables config file")
            return None
        connector_config = self.flink_tables_config.connector(database, connector_name)
        if not connector_config:
            logger.warn(f"connector {connector_name} does not exist in flink tables config file")
            return None
        return connector_config

    def _create_table(self, table: str, table_config: Dict, connector: Dict):
        schema = table_config["schema"]
        schema_expr = " , ".join(schema)
        partition_by_expr = (
            f"""
                PARTITIONED BY ({','.join(table_config['partition_by'])})"""
            if "partition_by" in table_config
            else ""
        )
        options = self.flink_tables_config.table_options(connector, table_config)
        options_expr = " , ".join([f"'{option}' = '{options[option]}'" for option in options])
        create_sql = f"""
            create table {table.strip()} (
                {schema_expr}
            )
            {partition_by_expr}
            WITH (
                {options_expr}
            );
        """
        self.exec_native_sql(create_sql)

    def register_tables(self, tables: List[str], include_catalog: bool = True):
        if self.flink_tables_config:
            if include_catalog:
                self._register_catalog()
            self._register_tables(tables)

    def add_jars(self, jars_path: List[str]):
        self.flink.get_config().set("pipeline.jars", f'{";".join([f"file://{path}" for path in jars_path])}')

    def set_configurations(self, configs: dict):
        for c in configs:
            self.flink.get_config().set(c, configs[c])


class FlinkTablesConfig:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config

    @staticmethod
    def from_config_file(config_file_path: Optional[str] = None) -> FlinkTablesConfig:
        if not config_file_path or not os.path.exists(config_file_path) or not os.path.isfile(config_file_path):
            logger.warn(
                "config file '{config_file_path}' does not exist or is not a file, will create an empty"
                " FlinkTablesConfig"
            )
            return FlinkTablesConfig({})
        with open(config_file_path, "r") as f:
            config = json.loads(f.read())
            return FlinkTablesConfig(config)

    @property
    def catalogs(self) -> List[Dict]:
        if "catalogs" in self.config:
            return self.config["catalogs"]
        else:
            return []

    @property
    def databases(self) -> List[Dict]:
        if "databases" in self.config:
            return self.config["databases"]
        else:
            return []

    def database(self, name: str) -> Optional[Dict]:
        return next(filter(lambda t: t["name"] == name, self.databases), None)

    def table(self, database_config: Dict, table_name: str) -> Optional[Dict]:
        return next(filter(lambda t: t["name"] == table_name, database_config["tables"]), None)

    def connector(self, database_config: Dict, connector_name: str) -> Optional[Dict]:
        connectors = database_config.get("connectors", [])
        return next(filter(lambda conn: conn["name"] == connector_name, connectors), None)

    def table_options(self, connector: Dict, table: Dict) -> Dict[str, str]:
        options = connector.get("options", {}).copy()
        tableOptions = table.get("connector", {}).get("options", {})
        options.update(tableOptions)
        if "path" in options and "path" not in tableOptions:
            assert "." not in table["name"], "Table name in configuration must not contains db, found " + table["name"]
            options["path"] = options["path"].rstrip("/") + f"/{table['name']}"
        return options

    def table_fields(self, full_table_name: str, exclude_fields: Optional[List[str]] = None) -> List[str]:
        exclude_fields = exclude_fields or []
        assert full_table_name.count(".") == 1, 'full_table_name must in format "db.table", found: ' + full_table_name
        db, table = tuple(full_table_name.split("."))
        db_config = self.database(db)
        if not db_config:
            raise Exception("db configuration not found for " + db)
        table_config = self.table(db_config, table)
        if not table_config:
            raise Exception("table configuration not found for " + full_table_name)
        if "schema" not in table_config:
            raise Exception("schema not found in table configuration for " + full_table_name)
        field_expr_list: List[str] = table_config["schema"]
        extract_field_name = lambda field_expr: re.split(r"\s", field_expr)[0].strip("'\"`")
        fields = [extract_field_name(field_expr) for field_expr in field_expr_list]
        return [f for f in fields if f not in exclude_fields]
