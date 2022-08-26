from typing import Dict, Callable, List, Tuple

import json
import os

from .base import *
from ...logger import logger
from pyflink.table import (EnvironmentSettings, TableEnvironment)
from pyflink.common import Row as PyFlinkRow
from pyflink.table.table_result import TableResult
from pyflink.table.catalog import ObjectPath
from ...udf import udfs

__all__ = [
    'FlinkRow', 'FlinkTable', 'FlinkBackend'
]


class FlinkRow(Row):

    def __init__(self, row: PyFlinkRow = None, fields: List[str] = None):
        self.row: PyFlinkRow = row
        if fields is not None:
            self.row._fields = fields

    def as_dict(self):
        return self.row.as_dict()

    def as_tuple(self) -> Tuple:
        return self.row

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
        from pyflink.table import Table
        self.table: Table = table

    def is_empty(self) -> bool:
        with self.table.limit(1).execute().collect() as result:
            collected_result = [item for item in result]
        return len(collected_result) == 0

    def field_names(self) -> List[str]:
        return self.table.get_schema().get_field_names()

    def first(self) -> 'Row':
        fields = self.table.get_schema().get_field_names()
        with self.table.execute().collect() as result:
            collected_result = [FlinkRow(item, fields) for item in result]
        return FlinkRow() if len(collected_result) == 0 else collected_result[0]

    def limit(self, count: int) -> 'FlinkTable':
        return FlinkTable(self.table.limit(count))

    def with_column(self, name: str, value: any) -> 'FlinkTable':
        from pyflink.table.expressions import lit
        from pyflink.table.expression import Expression
        return FlinkTable(self.table.add_columns((value if isinstance(value, Expression) else lit(value)).alias(name)))

    def collect(self) -> List['Row']:
        fields = self.table.get_schema().get_field_names()
        return [FlinkRow(item, fields) for item in self.table.execute().collect()]

    def show(self, count: int = 20):
        self.table.limit(count).execute().print()

    def count(self) -> int:
        with self.table.execute().collect() as result:
            collected_result = [item for item in result]
        return len(collected_result)

class FlinkBackend(Backend):

    # todo: 考虑是否需要在外面实例化flink: TableEnvironment
    def __init__(self, flink: TableEnvironment = None, scala_udf_initializer: str = None):
        self.flink: TableEnvironment = TableEnvironment.create(EnvironmentSettings.in_batch_mode())

    def init_udfs(self, scala_udf_initializer: str = None, *args, **kwargs):
        from pyflink.table import DataTypes
        from pyflink.table.udf import udf
        self.register_udfs({"test_func": udf(lambda i, j: i + j, result_type=DataTypes.BIGINT())})

    def register_udfs(self, funcs: Dict[str, Callable]):
        for key in funcs:
            func = funcs[key]
            self.flink.create_temporary_system_function(key, func)

    def clean(self):
        for temp_view in self.flink.list_temporary_views():
            self.flink.drop_temporary_view(temp_view)

    def exec_native_sql(self, sql: str) -> TableResult:
        logger.info(f'will exec sql: {sql}')
        return self.flink.execute_sql(sql)

    def exec_sql(self, sql: str) -> 'Table':
        logger.info(f'will exec sql: {sql}')
        return FlinkTable(self.flink.sql_query(sql))

    def create_empty_table(self):
        return FlinkTable('')

    def create_temp_table(self, table: 'Table', name: str):
        self.flink.create_temporary_view(name, table.table)

    def create_cache_table(self, table: 'Table', name: str):
        self.flink.create_temporary_view(name, table.table)

    def table_exists(self, table: 'TableMeta'):
        catalog = self.flink.get_current_catalog()
        database = self.flink.get_current_database()
        return self.flink.get_catalog(catalog).table_exists(ObjectPath(table.dbname if table.dbname else database, table.pure_table_name))

    def save_table(self, source_table_meta: 'TableMeta', target_table_meta: 'TableMeta', save_mode: 'SaveMode', create_target_table: bool = False):

        from pyflink.table.expressions import lit, col

        if not self.table_exists(target_table_meta):
            raise Exception(f'target table {target_table_meta.table_name} does not exist, '
                            f'cannot save table {target_table_meta.table_name} to {target_table_meta.table_name}')

        temp_res = self.flink.sql_query(f"select * from {source_table_meta.table_name}")
        # 纯动态分区时，如果当日没有新增数据，则不会创建 partition。而我们希望对于静态分区，总是应该创建分区，即使当日没有数据
        dynamic_partitions = list(filter(lambda p: not p.value, target_table_meta.partitions))
        static_partitions = list(filter(lambda p: p.value, target_table_meta.partitions))
        columns =self.flink.sql_query(f'select * from {target_table_meta.table_name}').limit(0).get_schema().get_field_names()
        for p in static_partitions:
            temp_res = temp_res.add_columns(lit(p.value).alias(p.field))
        temp_res = temp_res.select(*list(map(lambda column: col(column), columns)))

        temp_res.execute_insert(target_table_meta.table_name, save_mode == SaveMode.overwrite)

    def refresh_table_partitions(self, table: TableMeta):
        # flink无法从`desc table`中解析出partition字段，但是可以在flink_source_file中配置table的partition字段
        pass

    def _register_catalog(self, flink_config):
        assert flink_config['excution']['catalog']
        catalog = flink_config['excution']['catalog']
        if catalog:
            catalog_name = catalog['database']
            del catalog['database']
            catalog_expr = " , ".join(
                [f"'{option}' = '{catalog[option]}'" for option in catalog]
            )
            self.flink.execute_sql(f"""
                CREATE CATALOG {catalog_name} 
                WITH (
                    {catalog_expr}
                );
            """)
            self.flink.execute_sql(f'USE CATALOG {catalog_name}')

    def _register_tables(self, flink_config, tables: List[str]):
        if len(tables) == 0:
            return
        for database in flink_config['databases']:
            db_name = database['name']
            connectors = database['connectors']
            self.flink.execute_sql(f'create database if not exists {db_name}')
            for table in tables:
                table_config = next(filter(lambda t: t['name'] == table.strip().split('.')[1], database['tables']), None)
                if not table_config:
                    raise Exception(f'table {table} does not exist in table config file, register table failed.')

                connector = next(filter(lambda conn: conn['name'] == table_config['connector']['name'], connectors), None)
                schema = table_config['schema']
                schema_expr = " , ".join(schema)
                partition_by_expr = f"PARTITIONED BY ({','.join(table_config['partition_by'])})" if "partition_by" in table_config else ''
                options = dict()
                options.update(connector['options'])
                options.update(table_config['connector']['options'])
                options_expr = " , ".join(
                    [f"'{option}' = '{options[option]}'" for option in options]
                )
                print(partition_by_expr)
                create_sql = f"""
                    create table if not exists {table.strip()} (
                        {schema_expr}
                    )
                    {partition_by_expr}
                    WITH (
                        {options_expr}
                    );
                """
                self.flink.execute_sql(create_sql)

    def register_tables(self, flink_tables_file_path: str, tables: List[str]):
        if flink_tables_file_path and os.path.exists(flink_tables_file_path):
            with open(flink_tables_file_path, "r") as f:
                config = json.loads(f.read())
                self._register_catalog(config)
                self._register_tables(config, tables)
