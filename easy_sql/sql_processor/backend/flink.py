from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional, Tuple

from easy_sql.sql_processor.common import SqlProcessorException

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

        self.flink_tables_config = flink_tables_config or FlinkTablesConfig({}, {})

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

    def verify_schema(self, source_table: TableMeta, target_table: TableMeta, verify_type: bool = False):
        logger.info("Will not try verify schema in flink backend since flink will do a simple verify it for us.")

    def clean(self, dry_run: bool = False):
        if not dry_run:
            self.execute_streaming_inserts()

        for temp_view in self.flink.list_temporary_views():
            self.flink.drop_temporary_view(temp_view)

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

        catalog = self.flink.get_catalog(catalog)
        if not catalog:
            raise SqlProcessorException(
                f"Catalog {catalog} not exists when trying to check existence for table: {table}"
            )
        return catalog.table_exists(ObjectPath(database, table.pure_table_name))

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
        source_table_name = source_table_meta.table_name
        sink_table_name = target_table_meta.table_name
        if not self.table_exists(target_table_meta):
            raise Exception(
                f"target table {sink_table_name} does not exist, cannot save table {source_table_name} to"
                f" {sink_table_name}"
            )

        source_table: PyFlinkTable = self._align_fields(source_table_meta, target_table_meta)

        override_insert = save_mode == SaveMode.overwrite
        if self.streaming_insert_stmts is not None:
            logger.info(f"prepare insert into streaming_insert_stmts: from {source_table_name} to {sink_table_name}.")
            self.streaming_insert_stmts.add_insert(sink_table_name, source_table, overwrite=override_insert)
            self.has_streaming_insert_stmts = True
        else:
            logger.info(f"save table {source_table_name} to {sink_table_name}")
            source_table.execute_insert(sink_table_name, overwrite=override_insert)

    def _align_fields(self, source_table_meta: TableMeta, target_table_meta: TableMeta) -> PyFlinkTable:
        from pyflink.table.expressions import col, lit

        source_table = self.flink.from_path(source_table_meta.table_name)

        # 纯动态分区时，如果当日没有新增数据，则不会创建 partition。而我们希望对于静态分区，总是应该创建分区，即使当日没有数据
        static_partitions = list(filter(lambda p: p.value, target_table_meta.partitions))
        for p in static_partitions:
            source_table = source_table.add_columns(lit(p.value).alias(p.field))

        target_table_schema = list(self.exec_native_sql(f"desc {target_table_meta.table_name}").collect())
        # <Row('computed_field', 'BIGINT', True, None, 'AS `user` * `amount`', None)>
        target_needed_columns: List[str] = [f[0] for f in target_table_schema if f[4] is None]  # type: ignore

        source_table = source_table.select(*[col(column) for column in target_needed_columns])
        return source_table

    def refresh_table_partitions(self, table: TableMeta):
        # flink无法从`desc table`中解析出partition字段，但是可以在flink_source_file中配置table的partition字段
        pass

    def register_tables(self):
        for name, ddl in self.flink_tables_config.generate_catalog_ddl():
            exists = self.flink.get_catalog(name)
            if not exists:
                self.exec_native_sql(ddl)
        for ddl in self.flink_tables_config.generate_db_ddl():
            self.exec_native_sql(ddl)
        for ddl in self.flink_tables_config.generate_table_ddl():
            self.exec_native_sql(ddl)

    def add_jars(self, jars_path: List[str]):
        jars = f'{";".join([f"file://{os.path.abspath(path)}" for path in jars_path])}'
        self.flink.get_config().set("pipeline.jars", jars)
        self.flink.get_config().set("pipeline.classpaths", jars)

    def set_configurations(self, configs: dict):
        for c in configs:
            self.flink.get_config().set(c, configs[c])


@dataclass
class FlinkTablesConfig:
    connectors: Dict[str, Connector]
    catalogs: Dict[str, Catalog]

    @dataclass
    class Connector:
        options: str

        @staticmethod
        def from_dict(data: dict) -> FlinkTablesConfig.Connector:
            return FlinkTablesConfig.Connector(data.get("options", ""))

    @dataclass
    class Catalog:
        databases: Dict[str, FlinkTablesConfig.Database]
        temporary_tables: Dict[str, FlinkTablesConfig.Table]
        options: str | Dict[str, str]

        @staticmethod
        def from_dict(data: dict) -> FlinkTablesConfig.Catalog:
            options = data.get("options", "")
            databases = {
                key: FlinkTablesConfig.Database.from_dict(item) for key, item in data.get("databases", {}).items()
            }
            temporary_tables = {
                key: FlinkTablesConfig.Table.from_dict(item) for key, item in data.get("temporary_tables", {}).items()
            }

            return FlinkTablesConfig.Catalog(options=options, databases=databases, temporary_tables=temporary_tables)

    @dataclass
    class Database:
        tables: Dict[str, FlinkTablesConfig.Table]

        @staticmethod
        def from_dict(data: dict) -> FlinkTablesConfig.Database:
            tables = {key: FlinkTablesConfig.Table.from_dict(item) for key, item in data.get("tables", {}).items()}

            return FlinkTablesConfig.Database(tables=tables)

    @dataclass
    class Table:
        schema: str | List[str]
        options: str | Dict[str, str] | None = None
        partition_by: str | None = None
        connector: str | None = None

        @staticmethod
        def from_dict(data: dict) -> FlinkTablesConfig.Table:
            return FlinkTablesConfig.Table(**data)

        @property
        def normalized_schema(self) -> str:
            return self.schema if isinstance(self.schema, str) else ",\n".join(self.schema) + "\n"

    @staticmethod
    def from_file(file_path: str | None) -> FlinkTablesConfig:
        if file_path is None:
            logger.warn(f"config file '{file_path}' not specified, will create an empty FlinkTablesConfig")
            return FlinkTablesConfig({}, {})
        assert os.path.exists(file_path) and os.path.isfile(file_path), f"config file '{file_path}' not exists"
        if file_path.lower().endswith(".json"):
            return FlinkTablesConfig.from_json(file_path)
        else:
            if not (file_path.lower().endswith(".yml") or file_path.lower().endswith(".yaml")):
                logger.warn("Config file does not ends with .yml or .yaml, will try to load it as yaml format.")
            return FlinkTablesConfig.from_yml(file_path)

    @staticmethod
    def from_yml(file_path: str) -> FlinkTablesConfig:
        import yaml

        with Path(file_path).open(encoding="utf8") as f:
            res: dict = yaml.safe_load(f)
            return FlinkTablesConfig.from_dict(res)

    @staticmethod
    def from_json(file_path: str) -> FlinkTablesConfig:
        with Path(file_path).open(encoding="utf8") as f:
            res: dict = json.loads(f.read())
            return FlinkTablesConfig.from_dict(res)

    @staticmethod
    def from_dict(data: dict) -> FlinkTablesConfig:
        connectors = {
            key: FlinkTablesConfig.Connector.from_dict(item) for key, item in data.get("connectors", {}).items()
        }
        catalogs = {key: FlinkTablesConfig.Catalog.from_dict(item) for key, item in data.get("catalogs", {}).items()}

        return FlinkTablesConfig(connectors=connectors, catalogs=catalogs)

    def generate_catalog_ddl(self) -> Iterable[tuple[str, str]]:
        for name, catalog in self.catalogs.items():
            if isinstance(catalog.options, str):
                options = catalog.options
            elif isinstance(catalog.options, dict):
                options = ", ".join([f"'{k}' = '{catalog.options[k]}'" for k in sorted(catalog.options.keys())])
            else:
                raise Exception(f"Unknown catalog option (should be str or dict): {catalog.options}")
            ddl = f"CREATE CATALOG {name} with ({options})"
            yield name, ddl

    def generate_db_ddl(self) -> Iterable[str]:
        for cata_name, cata in self.catalogs.items():
            for db_name, _ in cata.databases.items():
                ddl = f"CREATE database if not exists {cata_name}.{db_name}"
                yield ddl

    def generate_table_ddl(self) -> Iterable[str]:
        for cata_name, cata in self.catalogs.items():
            for db_name, db in cata.databases.items():
                for tb_name, tb in db.tables.items():
                    full_tb_name = f"{cata_name}.{db_name}.{tb_name}"
                    ddl = self._generate_table_ddl(tb, full_tb_name)
                    yield ddl
            for tp_tb_name, tp_tb in cata.temporary_tables.items():
                ddl = self._generate_table_ddl(tp_tb, tp_tb_name, is_temporary=True)
                yield ddl

    def _generate_table_ddl(self, tb: FlinkTablesConfig.Table, tb_name: str, is_temporary=False) -> str:
        merged_options = self._merge_table_options(tb)
        options_expr = f"with ({merged_options})" if merged_options else ""

        partition_by_expr = f"partitioned by ({tb.partition_by})" if tb.partition_by else ""
        ddl = (
            f'create {"temporary" if is_temporary else ""} table if not exists {tb_name} ({tb.normalized_schema})'
            f" {partition_by_expr} {options_expr}"
        )
        return ddl

    def _merge_table_options(self, tb: FlinkTablesConfig.Table) -> str | None:
        base_options = {}
        assert (
            isinstance(tb.options, (str, dict)) or tb.options is None
        ), f"table options should be of type str or dict, found: {tb.options}"
        tb_options = (
            self.parse_options(tb.options or "") if isinstance(tb.options, str) or tb.options is None else tb.options
        )
        connector_name = tb.connector
        if connector_name:
            base_options = self.get_connector_options(connector_name)

        base_options.update(tb_options)
        merged_options = " , ".join([f"'{k}' = '{v}'" for k, v in base_options.items()])
        return merged_options

    def get_connector_options(self, name: str) -> Dict[str, str]:
        connector = self.connectors.get(name)
        if not connector:
            raise Exception(f"couldn't find connector {name} in {list(self.connectors.keys())}")
        options = self.parse_options(connector.options) if isinstance(connector.options, str) else connector.options
        return options

    @staticmethod
    def parse_options(option_str: str) -> Dict[str, str]:
        option_str = option_str or ""
        options = {}
        for line in option_str.splitlines():
            for each_option in line.split(","):
                if each_option.strip() == "":
                    continue
                assert (
                    "=" in each_option
                ), f"invalid option, must be of '{{key}}'='{{value}}', found '{each_option}' in line '{line}'"
                key, value = each_option[: each_option.index("=")], each_option[each_option.index("=") + 1 :]
                options[key.strip(" '")] = value.strip(" '")
        return options
