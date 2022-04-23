import re
import string
import time
from datetime import datetime, date
from random import random
from typing import Dict, Callable, List, Tuple, Optional, Any, Union

from ...logger import logger
from .base import *

__all__ = [
    'RdbBackend'
]

from .base import Col

from ...udf import udfs


class TimeLog:
    time_took_tpl = 'time took: {time_took:.3f}s'

    def __init__(self, start_log: str, end_log_tpl: str):
        self.start_log = start_log
        self.end_log_tpl = end_log_tpl
        self.start_dt = None

    def __enter__(self):
        logger.info(self.start_log)
        self.start_dt = datetime.now()

    def __exit__(self, exc_type, exc_val, exc_tb):
        time_took = (datetime.now() - self.start_dt).total_seconds()
        logger.info(self.end_log_tpl.format(time_took=time_took))


def _exec_sql(conn, sql: Union[str, List[str]], *args, **kwargs):
    from sqlalchemy.engine.base import Connection
    conn: Connection = conn
    with TimeLog(f'start to execute sql: {sql}, kwargs={kwargs}', f'end to execute sql({TimeLog.time_took_tpl}): {sql}'):
        if isinstance(sql, str):
            return conn.execute(sql, *args, **kwargs)
        else:
            execute_result = None
            for each_sql in sql:
                execute_result = conn.execute(each_sql, *args, **kwargs)
            return execute_result


_quote_str = lambda x: f"'{x}'" if isinstance(x, str) else f'{x}'


class SqlExpr:

    def __init__(self,
                 value_to_sql_expr: Callable[[Any], Optional[str]] = None,
                 column_sql_type_converter: Callable[[str, str, 'TypeEngine'], Optional[str]] = None,
                 partition_col_converter: Callable[[str], str] = None,
                 partition_value_converter: Callable[[str, str], Any] = None,
                 partition_expr: Callable[[str, str], str] = None):
        self.value_to_sql_expr = value_to_sql_expr
        self.column_sql_type_converter = column_sql_type_converter
        self.partition_col_converter = partition_col_converter
        self.partition_value_converter = partition_value_converter
        self.partition_expr = partition_expr

    def convert_partition_col(self, partition_col: str) -> str:
        if self.partition_col_converter:
            return self.partition_col_converter(partition_col)
        return partition_col

    def bigquery_partition_expr(self, partition_col: str) -> str:
        if self.partition_expr:
            return self.partition_expr('bigquery', partition_col)
        return partition_col

    def convert_partition_value(self, partition_col: str, value: str) -> Any:
        if self.partition_value_converter:
            return self.partition_value_converter(partition_col, value)
        return value

    def for_value(self, value: Union[str, int, float, datetime, date]) -> str:
        if self.value_to_sql_expr:
            sql_expr = self.value_to_sql_expr(value)
            if sql_expr is not None:
                return sql_expr

        if not isinstance(value, (str, int, float, datetime, date)):
            raise Exception(
                f'when create new columns, the current supported value types are [str, int, float, datetime, date], found: '
                f'value={value}, type={type(value)}')
        if isinstance(value, (str,)):
            return f"'{value}'"
        elif isinstance(value, (int, float,)):
            return f"{value}"
        elif isinstance(value, (datetime,)):
            return f"cast('{value.strftime('%Y-%m-%d %H:%M:%S')}' as timestamp)"
        elif isinstance(value, (date,)):
            return f"cast ('{value.strftime('%Y-%m-%d')}' as date)"
        else:
            raise Exception(f'value of type {type(value)} not supported yet!')

    def for_bigquery_type(self, col_name: str, col_type: Union[str, 'TypeEngine']) -> str:
        if self.column_sql_type_converter:
            converted_col_type = self.column_sql_type_converter('bigquery', col_name, col_type)
            if converted_col_type is not None:
                return converted_col_type

        if str(col_type.__class__) == "<class 'str'>":
            return col_type

        import sqlalchemy
        if isinstance(col_type, (sqlalchemy.FLOAT, sqlalchemy.Float)):
            return 'FLOAT64'
        elif isinstance(col_type, sqlalchemy.VARCHAR):
            return 'STRING'
        else:
            return col_type.__class__.__name__


class RdbTable(Table):

    def __init__(self, backend, sql: str, actions: List[Tuple] = None):
        self.backend: RdbBackend = backend
        self.db_config: DbConfig = backend.db_config
        self.sql = sql
        self._exec_sql = lambda sql: _exec_sql(self.backend.conn, sql)
        self._actions = actions or []

        self._temp_table_time_prefix = lambda: f't_{int(time.mktime(time.gmtime()))}_{int(random() * 10000):04d}'
        self._is_simple_query = lambda sql: re.match(r'^select \* from [\w.]+$', sql)
        self._table_name_of_simple_query = lambda sql: re.match(r'select \* from ([\w.]+)', sql).group(1)

    @staticmethod
    def from_table_meta(backend, table_meta: TableMeta):
        table = RdbTable(backend, f'select * from {table_meta.get_full_table_name(backend.temp_schema)}')
        if table_meta.has_partitions():
            for pt in table_meta.partitions:
                if pt.field not in table.field_names():
                    table = table.with_column(pt.field, backend.sql_expr.for_value(pt.value))
                else:
                    if pt.value is not None:
                        logger.warning(f'partition column already exists in table {table_meta.table_name}, '
                                       f'but right now we provided a new value {pt.value} for partition column {pt.field}. Will ignore it.')
        return table

    def _execute_actions(self):
        for action in self._actions:
            if action[0] == 'limit':
                count = action[1]
                with TimeLog(f'start to execute action: {action}',
                             f'end to execute action({TimeLog.time_took_tpl}): {action}'):
                    prefix = f'{self._temp_table_time_prefix()}'
                    limit_result_table_name = f'{prefix}_limit_{count}'
                    if self._is_simple_query(self.sql):
                        temp_table_name = self._table_name_of_simple_query(self.sql)
                        self._exec_sql(self.backend.db_config.create_view_sql(limit_result_table_name,
                                                                              f'select * from {temp_table_name} limit {count}'))
                    else:
                        temp_table_name = f'{prefix}_limit_{count}_source'
                        self._exec_sql(self.backend.db_config.create_view_sql(temp_table_name, self.sql))
                        self._exec_sql(self.backend.db_config.create_view_sql(limit_result_table_name,
                                                                              f'select * from {self.backend.temp_schema}.{temp_table_name} limit {count}'))
                    self.sql = f'select * from {self.backend.temp_schema}.{limit_result_table_name}'
            elif action[0] == 'newcol':
                name, value = action[1], action[2]
                with TimeLog(f'start to execute action: {action}',
                             f'end to execute action({TimeLog.time_took_tpl}): {action}'):
                    prefix = self._temp_table_time_prefix()
                    # for pg: max table name chars allowed is 63, the max length is 55 for newcol_table_name
                    newcol_table_name = f'{prefix}_newcol_{name[:30]}'
                    if self._is_simple_query(self.sql):
                        temp_table_name = self._table_name_of_simple_query(self.sql)
                        field_names = self._field_names(f"select * from {temp_table_name}")
                        select_sql = f'select {", ".join(field_names)}, {value} as {name} from {temp_table_name}'
                    else:
                        # for pg: max table name chars allowed is 63, the max length is 62 for newcol_table_name
                        temp_table_name = f'{prefix}_newcol_{name[:30]}_source'
                        self._exec_sql(self.db_config.create_view_sql(temp_table_name, self.sql))
                        field_names = self._field_names(f"select * from {self.backend.temp_schema}.{temp_table_name}")
                        select_sql = f'select {", ".join(field_names)}, {value} as {name} from {self.backend.temp_schema}.{temp_table_name}'

                    self._exec_sql(self.db_config.create_view_sql(newcol_table_name, select_sql))
                    self.sql = f'select * from {self.backend.temp_schema}.{newcol_table_name}'
            else:
                raise Exception(f'unsupported action: {action}')
        self._actions = []

    def is_empty(self) -> bool:
        return self.count() == 0

    def field_names(self) -> List[str]:
        self._execute_actions()
        return self._field_names(self.sql)

    def _field_names(self, sql: str) -> List[str]:
        from sqlalchemy.engine.result import ResultProxy
        result: ResultProxy = self._exec_sql(sql)
        result.close()
        return result.keys()

    def first(self) -> 'RdbRow':
        all_action_are_limit = all([action[0] == 'limit' for action in self._actions])
        if all_action_are_limit:
            min_limit = min([action[1] for action in self._actions]) if len(self._actions) > 0 else 1
            from sqlalchemy.engine.result import ResultProxy
            result: ResultProxy = self._exec_sql(self.sql)
            if min_limit <= 0:
                return RdbRow(result.keys(), None)
            with TimeLog(f'start to fetch first row: {self.sql}',
                         f'end to fetch first row({TimeLog.time_took_tpl}): {self.sql}'):
                row = result.first()
            return RdbRow(result.keys(), row)

        self._execute_actions()
        from sqlalchemy.engine.result import ResultProxy
        result: ResultProxy = self._exec_sql(self.sql)
        with TimeLog(f'start to fetch first row: {self.sql}',
                     f'end to fetch first row({TimeLog.time_took_tpl}): {self.sql}'):
            row = result.first()
        return RdbRow(result.keys(), row)

    def limit(self, count: int) -> 'RdbTable':
        return RdbTable(self.backend, self.sql, self._actions + [('limit', count)])

    def with_column(self, name: str, value: any) -> 'RdbTable':
        return RdbTable(self.backend, self.sql, self._actions + [('newcol', name, value)])

    def collect(self) -> List['RdbRow']:
        return self._collect()

    def _collect(self, row_count: int = None) -> List['RdbRow']:
        self._execute_actions()
        from sqlalchemy.engine.result import ResultProxy
        result: ResultProxy = self._exec_sql(self.sql)
        # collect at most 1000 rows for now
        max_rows = 1000 if row_count is None else row_count
        with TimeLog(f'start to fetch first row: {self.sql}',
                     f'end to fetch first row({TimeLog.time_took_tpl}): {self.sql}'):
            try:
                rows = result.fetchmany(max_rows)
            except Exception as e:
                print('result.fetchmany(max_rows) found error: ', e)
                if e.args[0] == "This result object does not return rows. It has been closed automatically.":
                    return []
                else:
                    raise e
        if row_count is None and len(rows) == max_rows:
            logger.warning(
                f'found {max_rows} items, but there may be more, will only fetch {max_rows} items at most for sql: {self.sql}')
        rows = [RdbRow(result.keys(), row) for row in rows]
        result.close()
        return rows

    def show(self, count: int = 20):
        keys = self.field_names()
        rows = self._collect(count)
        print('\t'.join(keys))
        for row in rows:
            print('\t'.join([_quote_str(item) for item in row]))

    def count(self) -> int:
        temp_table_name = self.resolve_to_temp_table()
        return self._exec_sql(f'select count(1) from {self.backend.temp_schema}.{temp_table_name}').first()[0]

    def resolve_to_temp_table(self) -> str:
        self._execute_actions()
        if self._is_simple_query(self.sql) and '.' not in self._table_name_of_simple_query(self.sql):
            temp_table_name = self._table_name_of_simple_query(self.sql)
        else:
            prefix = self._temp_table_time_prefix()
            temp_table_name = f'{prefix}_count'
            self._exec_sql(self.db_config.create_view_sql(temp_table_name, self.sql))
        self.sql = f'select * from {self.backend.temp_schema}.{temp_table_name}'
        return temp_table_name

    def save_to_temp_table(self, name: str):
        temp_table_name = self.resolve_to_temp_table()
        if temp_table_name != name:
            if '.' in name:
                raise Exception(f'renaming should only happen in temp database, '
                                f'so name must be pure TABLE_NAME when renaming tables, found: {name}')
            if temp_table_name != name:
                if self.backend.table_exists(TableMeta(name)):
                    raise Exception(
                        f'we are trying to replace an existing temp table, it is not supported right now. table name: {name}')
                self._exec_sql(
                    self.db_config.create_view_sql(name, f'select * from {self.backend.temp_schema}.{temp_table_name}'))

    def save_to_table(self, target_table: TableMeta):
        if self.backend.table_exists(target_table):
            raise Exception('does not support to save to an existing table')

        temp_table_name = self.resolve_to_temp_table()

        field_names = self.field_names()
        for pt in target_table.partitions:
            if pt.field not in field_names:
                raise Exception(
                    f'does not found partition field `{pt.field}` in source table for target table {target_table.table_name}, '
                    f'all fields are in source table: {field_names}')

        cols = self.backend.inspector.get_columns(temp_table_name, self.backend.temp_schema)
        db = target_table.table_name[:target_table.table_name.index('.')]
        self._exec_sql(self.db_config.create_db_sql(db))
        self._exec_sql(self.db_config.create_table_with_partitions_sql(target_table.table_name, cols, target_table.partitions))

        target_table_name = target_table.get_full_table_name(self.backend.temp_schema)
        if not self.db_config.create_partition_automatically():
            if target_table.partitions:
                partition_values = self.backend.exec_sql(f'select distinct {", ".join([p.field for p in target_table.partitions])} '
                                                         f'from {self.backend.temp_schema}.{temp_table_name}').collect()
                partitions_to_save = [[Partition(p.field, v[i]) for i, p in enumerate(target_table.partitions)] for v in partition_values]
            else:
                partitions_to_save = []
            source_table_name = f'{self.backend.temp_schema}.{temp_table_name}'
            sqls = self.db_config.create_partitions_sqls(source_table_name, target_table_name, [col['name'] for col in cols], partitions_to_save)
            for sql in sqls:
                self._exec_sql(sql)
        else:
            cols = [col['name'] for col in cols]
            col_names = ', '.join(cols)
            converted_col_names = ", ".join(self.db_config.convert_col(cols, [pt.field for pt in target_table.partitions]))
            self._exec_sql(self.db_config.insert_data_sql(target_table_name, col_names,
                                                          f'select {converted_col_names} from {self.backend.temp_schema}.{temp_table_name}',
                                                          target_table.partitions))


class RdbRow(Row):

    def __init__(self, cols: List[str], values: Optional[Tuple]):
        self._cols = cols
        from decimal import Decimal
        # case decimal to float in order for later comparing (to ensure type consistency)
        self._values = tuple([float(v) if isinstance(v, Decimal) else v for v in values])

    def as_dict(self):
        return None if self._values is None else dict(zip(self._cols, self._values))

    def as_tuple(self):
        return self._values

    def __eq__(self, other: 'RdbRow'):
        if not isinstance(other, (RdbRow, tuple,)) or other is None:
            return False
        if isinstance(other, RdbRow):
            return other._cols == self._cols and other._values == self._values
        elif isinstance(other, tuple):
            return other == self._values

    def __str__(self):
        return f'({", ".join([f"{k}={_quote_str(v)}" for k, v in zip(self._cols, self._values)])})'

    def __getitem__(self, i):
        return self._values[i]

    def __repr__(self):
        return 'RdbRow' + str(self)


class DbConfig:

    def __init__(self, sql_expr: SqlExpr):
        self.sql_expr = sql_expr

    def create_partition_automatically(self) -> bool:
        raise NotImplementedError()

    def create_db_sql(self, db: str) -> str:
        raise NotImplementedError()

    def use_db_sql(self, db: str) -> str:
        raise NotImplementedError()

    def drop_db_sql(self, db: str) -> str:
        raise NotImplementedError()

    def rename_table_sql(self, from_table: str, to_table: str) -> str:
        raise NotImplementedError()

    def rename_table_db_sql(self, table_name: str, schema: str):
        raise NotImplementedError()

    def get_tables_sql(self, db) -> str:
        raise NotImplementedError()

    def create_table_sql(self, table_name: str, select_sql: str) -> str:
        raise NotImplementedError()

    def rename_view_sql(self, from_table: str, to_table: str) -> str:
        raise NotImplementedError()

    def drop_view_sql(self, table: str) -> str:
        raise NotImplementedError()

    def create_view_sql(self, table_name: str, select_sql: str) -> str:
        raise NotImplementedError()

    def support_native_partition(self) -> bool:
        raise NotImplementedError()

    def delete_partition_sql(self, table_name, partitions: List[Partition]) -> str:
        raise NotImplementedError()

    def native_partitions(self, table_name: str) -> Tuple[str, Callable]:
        raise NotImplementedError()

    def create_table_with_partitions_sql(self, table_name: str, cols: List[Dict], partitions: List[Partition]):
        raise NotImplementedError()

    def create_partitions_sqls(self, source_table_name: str, target_table_name: str, col_names: List[str], partitions: List[List[Partition]]):
        raise NotImplementedError()

    def create_partition_sql(self, target_table_name: str, partitions: List[Partition], if_not_exists: bool = False) -> str:
        raise NotImplementedError()

    def convert_col(self, cols: List[str], partitions: List[str]) -> List[str]:
        if len(partitions) == 0:
            return cols
        else:
            # the format of pt_col may be :{pt_col}, which is transferred by method create_table_with_data
            return [col if col not in [partitions[0], f":{partitions[0]}"] else self.sql_expr.convert_partition_col(col)
                    for col in cols]

    def insert_data_sql(self, table_name: str, col_names_expr: str, select_sql: str, partitions: List[Partition]) -> Union[str, List[str]]:
        raise NotImplementedError()

    def drop_table_sql(self, table: str):
        raise NotImplementedError()

    def support_static_partition(self):
        raise NotImplementedError()

    def create_pt_meta_table(self, db: str):
        raise NotImplementedError()


class BqDbConfig(DbConfig):

    def __init__(self, db: str, sql_expr: SqlExpr):
        self.db = db
        super().__init__(sql_expr)

    def create_db_sql(self, db: str) -> str:
        return f'create schema if not exists {db}'

    # There's no such equivalent statement like 'use ${db}' in BigQuery.
    # Table must be qualified with a dataset when using BigQuery
    def use_db_sql(self, db: str) -> str:
        return f'select 1'

    def drop_db_sql(self, db: str) -> str:
        return f'drop schema if exists {db} cascade'

    def rename_table_sql(self, from_table: str, to_table: str) -> str:
        from_full_table_name = from_table if self.contain_db(from_table) else f'{self.db}.{from_table}'
        to_pure_table_name = to_table.split(".")[1] if self.contain_db(to_table) else {to_table}
        return f'alter table if exists {from_full_table_name} rename to {to_pure_table_name}'

    # There's no statement that could change the dataset directly
    # Copy cannot be operated at view
    def rename_table_db_sql(self, table_name: str, schema: str):
        return f'create table if not exists {schema}.{table_name} copy {self.db}.{table_name}'

    def get_tables_sql(self, db) -> str:
        return f'select table_name from {db}.INFORMATION_SCHEMA.TABLES'

    def create_table_sql(self, table_name: str, select_sql: str) -> str:
        return f'create table if not exists {self.db}.{table_name} as {select_sql}'

    # Cannot rename view directly in BigQuery
    def rename_view_sql(self, from_table: str, to_table: str) -> str:
        return f'create view if not exists {self.db}.{to_table} as select * from {self.db}.{from_table}'

    def drop_view_sql(self, table: str) -> str:
        return f'drop view if exists {self.db}.{table}'

    def create_view_sql(self, table_name: str, select_sql: str) -> str:
        return f'create view if not exists {self.db}.{table_name} as {select_sql}'

    def support_native_partition(self) -> bool:
        return True

    def delete_partition_sql(self, table_name, partitions: List[Partition]) -> str:
        if not self.contain_db(table_name):
            raise Exception("BigQuery table must be qualified with a dataset.")
        else:
            db, pure_table_name = tuple(table_name.split('.'))

        if not partitions or len(partitions) == 0:
            raise Exception(
                f'cannot delete partition when partitions not specified: table_name={table_name}, partitions={partitions}')
        if len(partitions) > 1:
            raise Exception('BigQuery only supports single-column partitioning.')
        pt_expr = f"""{partitions[0].field} = '{partitions[0].value}'"""

        delete_pt_sql = f"delete {db}.{pure_table_name} where {pt_expr};"
        delete_pt_metadata = f"delete {db}.__table_partitions__ where table_name = '{pure_table_name}' and partition_value = '{partitions[0].value}';"
        return self.transaction(f'{delete_pt_sql}\n{delete_pt_metadata}')

    def native_partitions(self, table_name: str) -> Tuple[str, Callable]:
        db = table_name.split(".")[0] if self.contain_db(table_name) else {self.db}
        pure_table_name = table_name.split(".")[1] if self.contain_db(table_name) else {table_name}
        return f"select ddl from {db}.INFORMATION_SCHEMA.TABLES where table_name = '{pure_table_name}'", self.extract_partition_cols

    def extract_partition_cols(self, native_partitions_sql_result):
        create_table_sql_lines = native_partitions_sql_result.fetchall()[0][0].split('\n')
        pt_cols = []
        for line in create_table_sql_lines:
            if line.startswith('PARTITION BY '):
                pt_cols = [line[len('PARTITION BY '):].strip()]
                break
        return pt_cols

    def create_table_with_partitions_sql(self, table_name: str, cols: List[Dict[str, 'TypeEngine']], partitions: List[Partition]):
        cols_expr = f',\n'.join(f"{col['name']} {self.sql_expr.for_bigquery_type(col['name'], col['type'])}" for col in cols)
        if len(partitions) == 0:
            partition_expr = ''
        elif len(partitions) == 1:
            return f'partition by {self.sql_expr.bigquery_partition_expr(partitions[0].field)}'
        else:
            raise Exception('BigQuery only supports single-column partitioning.')
        table_name_with_db = table_name if self.contain_db(table_name) else f'{self.db}.{table_name}'
        return f'create table if not exists {table_name_with_db} (\n{cols_expr}\n)\n{partition_expr}\n'

    def create_partition_automatically(self):
        return True

    def insert_data_sql(self, table_name: str, col_names_expr: str, select_sql: str, partitions: List[Partition]):
        if not self.contain_db(table_name):
            raise Exception("BigQuery table must be qualified with a dataset.")
        else:
            db, pure_table_name = tuple(table_name.split('.'))
        insert_date_sql = f"insert into {table_name}({col_names_expr}) {select_sql};"
        insert_pt_metadata = f"insert into {db}.__table_partitions__ values('{pure_table_name}', '{partitions[0].value}', CURRENT_TIMESTAMP());" \
            if len(partitions) != 0 else ''
        return self.transaction(f'{insert_date_sql}\n{insert_pt_metadata}')

    def drop_table_sql(self, table: str):
        if not self.contain_db(table):
            raise Exception("BigQuery table must be qualified with a dataset.")
        else:
            db, pure_table_name = tuple(table.split('.'))
        drop_table_sql = f"drop table if exists {db}.{pure_table_name};"
        drop_pt_metadata = f"delete {db}.__table_partitions__ where table_name = '{pure_table_name}';"
        return f'{drop_table_sql}\n{drop_pt_metadata}'

    def support_static_partition(self):
        return False

    def create_pt_meta_table(self, db: str):
        return f"""create table if not exists {db}.__table_partitions__(
        table_name string, partition_value string, last_modified_time timestamp)
        cluster by table_name;"""

    @staticmethod
    def transaction(statement: str):
        return f"BEGIN TRANSACTION;\n{statement}\nCOMMIT TRANSACTION;"

    @staticmethod
    def contain_db(table_name: str):
        return "." in table_name


class PostgrePartition:

    def __init__(self, name: str, value: Union[int, str]):
        self.name = name
        self.value = value
        self.value_next = value + '_' if isinstance(value, str) else value + 1
        self.value_expr = f"'{value}'" if isinstance(value, str) else str(value)
        self.value_next_expr = f"'{self.value_next}'" if isinstance(self.value_next, str) else str(self.value_next)
        self.table_suffix = str(value).lower().replace('-', '_')

    @property
    def field_name(self):
        return self.name

    def partition_table_name(self, table_name: str):
        return f'{table_name}__{self.table_suffix}'


class PgDbConfig(DbConfig):

    def create_db_sql(self, db: str) -> str:
        return f'create schema if not exists {db}'

    def use_db_sql(self, db: str) -> str:
        return f"set search_path='{db}'"

    def drop_db_sql(self, db: str):
        return f'drop schema if exists {db} cascade'

    def rename_table_sql(self, from_table: str, to_table: str) -> str:
        pure_to_table = to_table if '.' not in to_table else to_table[to_table.index('.') + 1:]
        return f'alter table {from_table} rename to {pure_to_table}'

    def rename_table_db_sql(self, table_name: str, schema: str):
        return f'alter table {table_name} set schema {schema}'

    def rename_view_sql(self, from_table: str, to_table: str) -> str:
        return f'alter view {from_table} rename to {to_table}'

    def create_view_sql(self, table_name: str, select_sql: str) -> str:
        return f'create view {table_name} as {select_sql}'

    def drop_view_sql(self, table_name: str) -> str:
        return f'drop view {table_name} cascade'

    def get_tables_sql(self, db) -> str:
        return f"select tablename FROM pg_catalog.pg_tables where schemaname='{db}' union " \
               f"select viewname FROM pg_catalog.pg_views where schemaname='{db}'"

    def create_table_sql(self, table_name: str, select_sql: str) -> str:
        return f'create table {table_name} as {select_sql}'

    def support_native_partition(self) -> bool:
        return True

    def delete_partition_sql(self, table_name, partitions: List[Partition]) -> str:
        if len(partitions) > 1:
            raise Exception(f'Only support exactly one partition column, found: {[str(p) for p in partitions]}')
        partition = PostgrePartition(partitions[0].field, partitions[0].value)
        return f'drop table if exists {partition.partition_table_name(table_name)}'

    def native_partitions(self, table_name: str) -> Tuple[str, Callable]:
        if '.' not in table_name:
            raise Exception('table name must be of format {DB}.{TABLE} when query native partitions, found: ' + table_name)

        db, table = table_name[:table_name.index('.')], table_name[table_name.index('.') + 1:]
        sql = f'''
        SELECT pg_catalog.pg_get_partkeydef(pt_table.oid) as partition_key
        FROM pg_class pt_table
            JOIN pg_namespace nmsp_pt_table   ON nmsp_pt_table.oid  = pt_table.relnamespace
        WHERE nmsp_pt_table.nspname='{db}' and pt_table.relname='{table}'
        '''
        return sql, self.extract_partition_cols

    def extract_partition_cols(self, native_partitions_sql_result):
        partition_values = [v[0] for v in native_partitions_sql_result.fetchall()]
        if not partition_values:
            raise Exception('no partition values found, table may not exist')
        partition_value = partition_values[0]
        if partition_value is None:
            return []
        partition_value: str = partition_value
        if not partition_value.upper().startswith('RANGE (') or not partition_value.endswith(')'):
            raise Exception('unable to parse partition: ' + partition_value)
        return partition_value[len('RANGE ('): -1].split(',')

    def create_table_with_partitions_sql(self, table_name: str, cols: List[Dict], partitions: List[Partition]):
        if len(partitions) > 1:
            raise Exception(f'Only support exactly one partition column, found: {[str(p) for p in partitions]}')
        cols_expr = ',\n'.join([f"{col['name']} {col['type']}" for col in cols])
        partition_expr = f'partition by range({partitions[0].field})' if len(partitions) == 1 else ''
        return f'create table {table_name} (\n{cols_expr}\n) {partition_expr}'

    def create_partition_automatically(self):
        return False

    def create_partition_sql(self, target_table_name: str, partitions: List[Partition], if_not_exists: bool = False) -> str:
        if len(partitions) > 1:
            raise Exception(f'Only support exactly one partition column, found: {[str(p) for p in partitions]}')
        partition = PostgrePartition(partitions[0].field, partitions[0].value)
        partition_table_name = partition.partition_table_name(target_table_name)
        if_not_exists = 'if not exists' if if_not_exists else ''
        return f'create table {if_not_exists} {partition_table_name} partition of {target_table_name} ' \
               f'for values from ({partition.value_expr}) to ({partition.value_next_expr})'

    def create_partitions_sqls(self, source_table_name: str, target_table_name: str, col_names: List[str], partitions: List[List[Partition]]):
        col_names_expr = ', '.join(col_names)
        if not partitions:
            return [f'insert into {target_table_name}({col_names_expr}) select {col_names_expr} from {source_table_name}']
        sqls = []
        for partition in partitions:
            if len(partition) > 1:
                raise Exception(f'Only support exactly one partition column, found: {[str(p) for p in partitions]}')
            partition = PostgrePartition(partition[0].field, partition[0].value)
            partition_table_name = partition.partition_table_name(target_table_name)
            temp_table_name = f'{partition_table_name}__temp'
            sqls.append(f'drop table if exists {temp_table_name}')
            sqls.append(f'create table {temp_table_name} (like {target_table_name} including defaults including constraints)')
            sqls.append(f'alter table {temp_table_name} add constraint p '
                        f'check ({partition.field_name} >= {partition.value_expr} and {partition.field_name} < {partition.value_next_expr})')
            filter_expr = f'{partition.field_name} = {partition.value_expr}'
            sqls.append(f'insert into {temp_table_name}({col_names_expr}) select {col_names_expr} from {source_table_name} where {filter_expr}')
            sqls.append(f'drop table if exists {partition_table_name}')
            sqls.append(f'alter table {temp_table_name} rename to {partition_table_name[partition_table_name.index(".") + 1:]}')
            sqls.append(f'alter table {target_table_name} attach partition {partition_table_name} '
                        f'for values from ({partition.value_expr}) to ({partition.value_next_expr})')
            sqls.append(f'alter table {partition_table_name} drop constraint p')
        return sqls

    def insert_data_sql(self, table_name: str, col_names_expr: str, select_sql: str, partitions: List[Partition]):
        return f'insert into {table_name}({col_names_expr}) {select_sql}'

    def drop_table_sql(self, table: str):
        return f'drop table if exists {table}'

    def support_static_partition(self):
        return True


class ChDbConfig(DbConfig):
    def __init__(self, sql_expr: SqlExpr, partitions_table_name: string):
        self.partitions_table_name = partitions_table_name
        super(ChDbConfig, self).__init__(sql_expr)

    def create_db_sql(self, db: str):
        return f'create database if not exists {db}'

    def use_db_sql(self, db: str):
        return f'use {db}'

    def drop_db_sql(self, db: str):
        return f'drop database if exists {db}'

    def rename_table_sql(self, from_table: str, to_table: str) -> str:
        return f'rename table {from_table} to {to_table}'

    def rename_table_db_sql(self, table_name: str, schema: str):
        return f'rename table {table_name} to {schema}.{table_name}'

    def rename_view_sql(self, from_table: str, to_table: str) -> str:
        return self.rename_table_sql(from_table, to_table)

    def drop_view_sql(self, table: str) -> str:
        return f'drop table {table}'

    def create_view_sql(self, table_name: str, select_sql: str) -> str:
        return f'create view {table_name} as {select_sql}'

    def get_tables_sql(self, db: str):
        return f'show tables in {db}'

    def create_table_sql(self, table_name: str, select_sql: str):
        return f'create table {table_name} engine=MergeTree order by tuple() as {select_sql}'

    def support_native_partition(self) -> bool:
        return True

    def delete_partition_sql(self, table_name, partitions: List[Partition]) -> str:
        if not partitions or len(partitions) == 0:
            raise Exception(
                f'cannot delete partition when partitions not specified: table_name={table_name}, partitions={partitions}')
        pt_expr = f'tuple({", ".join([self.sql_expr.for_value(pt.value) for pt in partitions])})'
        return f'alter table {table_name} drop partition {pt_expr}'

    def native_partitions(self, table_name: str) -> Tuple[str, Callable]:
        return f'show create table {table_name}', self.extract_partition_cols

    def extract_partition_cols(self, native_partitions_sql_result):
        create_table_sql_lines = native_partitions_sql_result.fetchall()[0][0].split('\n')
        pt_cols = []
        for line in create_table_sql_lines:
            if line.startswith('PARTITION BY ('):
                pt_cols = [col.strip() for col in line[len('PARTITION BY ('):-1].split(',')]
                break
            elif line.startswith('PARTITION BY '):
                pt_cols = [line[len('PARTITION BY '):].strip()]
                break
        return pt_cols

    def create_table_with_partitions_sql(self, table_name: str, cols: List[Dict], partitions: List[Partition]):
        cols_expr = ',\n'.join([f"{col['name']} {col['type']}" for col in cols])
        if len(partitions) == 0:
            partition_expr = ''
        elif len(partitions) == 1:
            partition_expr = f'partition by {partitions[0].field}'
        else:
            partition_expr = f'partition by tuple({", ".join([pt.field for pt in partitions])})'
        return f'create table if not exists {table_name} (\n{cols_expr}\n)\nengine=MergeTree\n{partition_expr}\norder by tuple()'

    def create_partition_automatically(self):
        return True

    def insert_data_sql(self, table_name: str, col_names_expr: str, select_sql: str, partitions: List[Partition]) -> List[str]:
        insert_date_sql = f"insert into {table_name}({col_names_expr}) {select_sql};"

        if len(partitions) != 0:
            if len(partitions) > 1:
                raise Exception("for now clickhouse backend only support table with single field partition")
            db = table_name.split('.')[0]
            insert_pt_metadata = f"insert into {self.partitions_table_name} values({db}, {table_name}, {partitions[0].value}, now());"
            return [insert_date_sql, insert_pt_metadata]
        return [insert_date_sql]

    def drop_table_sql(self, table: str):
        drop_table_sql = f'drop table if exists {table}'
        db = table.split('.')[0]
        drop_pt_metadata = f"delete {self.partitions_table_name} where db_name = {db} and table_name = {table}"
        return [drop_table_sql, drop_pt_metadata]

    def support_static_partition(self):
        return False

    def create_pt_meta_table(self, db: str):
        # As clickhouse has create partition table in RdbBackend, no need to create again
        return 'select 1'


class RdbBackend(Backend):
    """table_partitions_table_name; means the table name which save the static partition info for all partition tables in data warehouse,
    for now need support backend type: [clickhouse]
    others backend has another method to manage static partition info or just support static partition"""

    def __init__(self, url: str, credentials: str = None, sql_expr: SqlExpr = None,
                 partitions_table_name='dataplat.__table_partitions__'):
        self.partitions_table_name = partitions_table_name
        self.url, self.credentials = url, credentials
        self.sql_expr = sql_expr or SqlExpr()
        self.__init_inner(self.url, self.credentials)

    def __init_inner(self, url: str, credentials: str = None):
        from sqlalchemy import create_engine
        from sqlalchemy.engine.base import Engine, Connection

        self.temp_schema = f'sp_temp_{int(time.mktime(time.gmtime()))}_{int(random() * 10000):04d}'

        self.backend_type, self.is_pg, self.is_ch, self.is_bq = None, False, False, False
        self.db_config: DbConfig = None
        if url.startswith('postgresql://'):
            self.backend_type, self.is_pg = 'pg', True
            self.db_config = PgDbConfig(self.sql_expr)
            self.engine: Engine = create_engine(url, isolation_level="AUTOCOMMIT", pool_size=1)
            self.conn: Connection = self.engine.connect()
            _exec_sql(self.conn, self.db_config.create_db_sql(self.temp_schema))
            _exec_sql(self.conn, self.db_config.use_db_sql(self.temp_schema))
        elif url.startswith('clickhouse://') or url.startswith('clickhouse+native://'):
            self.backend_type, self.is_ch = 'ch', True
            self.db_config = ChDbConfig(self.sql_expr, self.partitions_table_name)

            engine: Engine = create_engine(url, pool_size=1)
            conn: Connection = engine.connect()
            _exec_sql(conn, self.db_config.create_db_sql(self.temp_schema))

            self._create_partitions_table(conn)

            conn.close()

            url_parts = url.split('?')
            url_params = '' if len(url_parts) == 1 else f'?{url_parts[1]}'
            url_raw_parts = url_parts[0].split('/')
            if len(url_raw_parts) == 4:  # db in url
                url_raw_parts = url_raw_parts[: -1]
            elif len(url_raw_parts) == 3:  # db not in url
                url_raw_parts = url_raw_parts
            else:
                raise Exception(f'unrecognized url: {url}')
            url = f'{"/".join(url_raw_parts + [self.temp_schema])}{url_params}'

            self.engine: Engine = create_engine(url, pool_size=1)
            self.conn: Connection = self.engine.connect()
        elif url.startswith('bigquery://'):
            self.backend_type, self.is_bq = 'bq', True
            self.db_config = BqDbConfig(self.temp_schema, self.sql_expr)
            self.engine: Engine = create_engine(url, credentials_path=credentials)
            self.conn: Connection = self.engine.connect()
            _exec_sql(self.conn, self.db_config.create_db_sql(self.temp_schema))

    def _create_partitions_table(self, conn):
        cols = [
            {'name': 'db_name', 'type': 'String'},
            {'name': 'table_name', 'type': 'String'},
            {'name': 'partition_value', 'type': 'String'},
            {'name': 'last_modified_time', 'type': 'DateTime'}
        ]
        partitions = [Partition(field='db_name')]
        db_name = self.partitions_table_name.split('.')[0]
        _exec_sql(conn, self.db_config.create_db_sql(db_name))
        _exec_sql(conn, self.db_config.create_table_with_partitions_sql(self.partitions_table_name, cols, partitions))

    @property
    def inspector(self):
        from sqlalchemy import inspect
        from sqlalchemy.engine.reflection import Inspector
        # inspector object has cache built-in, so we should recreate the object if required
        inspector: Inspector = inspect(self.engine)
        return inspector

    def reset(self):
        self.__init_inner(self.url, self.credentials)

    def init_udfs(self, *args, **kwargs):
        self.register_udfs(udfs.get_udfs(self.backend_type))

    def register_udfs(self, funcs: Dict[str, Callable[[], Union[str, List[str]]]]):
        for udf_sql_creator in funcs.values():
            sql = udf_sql_creator()
            sqls = sql if isinstance(sql, list) else [sql]
            for sql in sqls:
                _exec_sql(self.conn, sql)

    def create_empty_table(self):
        return RdbTable(self, '')

    def exec_native_sql(self, sql: str) -> Any:
        return _exec_sql(self.conn, sql)

    def exec_sql(self, sql: str) -> 'RdbTable':
        return RdbTable(self, sql)

    def _tables(self, db: str) -> List[str]:
        all_tables = _exec_sql(self.conn, self.db_config.get_tables_sql(db)).fetchall()
        return sorted([table[0] for table in all_tables])

    def temp_tables(self) -> List[str]:
        return self._tables(self.temp_schema)

    def clear_cache(self):
        pass

    def clean(self):
        logger.info(f'clean temp db: {self.temp_schema}')
        _exec_sql(self.conn, self.db_config.drop_db_sql(self.temp_schema))

    def clear_temp_tables(self, exclude: List[str] = None):
        from sqlalchemy.exc import ProgrammingError
        for table in self.temp_tables():
            if table not in exclude:
                print(f'dropping temp table {table}')
                try:
                    _exec_sql(self.conn, self.db_config.drop_view_sql(table))
                except ProgrammingError as e:
                    if re.match(r'.*view ".*" does not exist', e.args[0]):
                        # Since we will drop view cascade in pg, so some view might already be dropped.
                        # It will raise the view-not-exist error, we just ignore this kind of error.
                        pass
                    else:
                        raise e

    def create_temp_table(self, table: 'RdbTable', name: str):
        logger.info(f'create_temp_table with: table={table}, name={name}')
        table.save_to_temp_table(name)

    def create_cache_table(self, table: 'RdbTable', name: str):
        logger.info(f'create_cache_table with: table={table}, name={name}')
        table.save_to_temp_table(name)

    def broadcast_table(self, table: 'RdbTable', name: str):
        logger.info(f'broadcast_table with: table={table}, name={name}')
        table.save_to_temp_table(name)

    def table_exists(self, table: 'TableMeta'):
        schema, table_name = table.dbname, table.pure_table_name
        schema = schema or self.temp_schema
        return table_name in self._tables(schema)

    def refresh_table_partitions(self, table: 'TableMeta'):
        if self.db_config.support_native_partition():
            native_partitions_sql, extract_partition_cols = self.db_config.native_partitions(table.table_name)
            pt_cols = extract_partition_cols(_exec_sql(self.conn, native_partitions_sql))
            table.update_partitions([Partition(col) for col in pt_cols])
        # no need to do anything, if the db does not support partition

    def save_table(self, source_table: 'TableMeta', target_table: 'TableMeta', save_mode: 'SaveMode',
                   create_target_table: bool):
        logger.info(f'save table with: source_table={source_table}, target_table={target_table}, '
                    f'save_mode={save_mode}, create_target_table={create_target_table}')

        if not self.db_config.support_static_partition():
            _exec_sql(self.conn, self.db_config.create_pt_meta_table(target_table.dbname))

        if not self.table_exists(target_table) and not create_target_table:
            raise Exception(f'target table {target_table.table_name} does not exist, and create_target_table is False, '
                            f'cannot save table {source_table.table_name} to {target_table.table_name}')

        source_table = source_table.clone_with_partitions(target_table.partitions)
        if not self.table_exists(target_table) and create_target_table:
            RdbTable.from_table_meta(self, source_table).save_to_table(target_table)
            return

        inspector = self.inspector
        target_cols = inspector.get_columns(target_table.pure_table_name, target_table.dbname or self.temp_schema)
        original_source_table = source_table
        source_table = TableMeta(RdbTable.from_table_meta(self, source_table).resolve_to_temp_table())
        source_cols = inspector.get_columns(source_table.pure_table_name, source_table.dbname or self.temp_schema)
        logger.info(f'ensure cols match for source_table {source_table.table_name} and target_table {target_table.table_name}')
        self._ensure_contain_target_cols(source_cols, target_cols)

        full_target_table_name = target_table.get_full_table_name(self.temp_schema)
        if save_mode == SaveMode.overwrite:
            # write data to temp table to support the case when read from and write to the same table
            temp_table_name = f'{full_target_table_name}__temp'
            _exec_sql(self.conn, self.db_config.drop_table_sql(temp_table_name))
            RdbTable.from_table_meta(self, source_table).save_to_table(target_table.clone_with_name(temp_table_name))
            if original_source_table.has_partitions():
                if any([pt.value is None for pt in original_source_table.partitions]):  # dynamic partitions (partition retrieved from real table)
                    pt_cols = [pt.field for pt in original_source_table.partitions]
                    pt_values_list = _exec_sql(self.conn, f'select distinct {", ".join(pt_cols)} '
                                                          f'from {source_table.get_full_table_name(self.temp_schema)}').fetchall()
                    for pt_values in pt_values_list:
                        partitions = [Partition(field, value) for field, value in zip(pt_cols, pt_values)]
                        _exec_sql(self.conn, self.db_config.delete_partition_sql(target_table.table_name, partitions))
                        if not self.db_config.create_partition_automatically():
                            _exec_sql(self.conn, self.db_config.create_partition_sql(full_target_table_name, partitions))
                else:  # static partitions (partition specified in sql file)
                    _exec_sql(self.conn, self.db_config.delete_partition_sql(target_table.table_name, original_source_table.partitions))
                    if not self.db_config.create_partition_automatically():
                        _exec_sql(self.conn, self.db_config.create_partition_sql(full_target_table_name, original_source_table.partitions))
                col_names = ', '.join([col['name'] for col in target_cols])
                _exec_sql(self.conn, self.db_config.insert_data_sql(full_target_table_name, col_names,
                                                                    f'select {col_names} from {temp_table_name}', target_table.partitions))
                _exec_sql(self.conn, self.db_config.drop_table_sql(temp_table_name))
            else:
                _exec_sql(self.conn, self.db_config.drop_table_sql(full_target_table_name))
                _exec_sql(self.conn, self.db_config.rename_table_sql(temp_table_name, full_target_table_name))
        elif save_mode == SaveMode.append:
            col_names = ', '.join([col['name'] for col in target_cols])
            if not self.db_config.create_partition_automatically():
                if original_source_table.has_partitions():
                    pt_cols = [pt.field for pt in original_source_table.partitions]
                    pt_values_list = _exec_sql(self.conn, f'select distinct {", ".join(pt_cols)} '
                                                          f'from {source_table.get_full_table_name(self.temp_schema)}').fetchall()
                    for pt_values in pt_values_list:
                        partitions = [Partition(field, value) for field, value in zip(pt_cols, pt_values)]
                        if not self.db_config.create_partition_automatically():
                            _exec_sql(self.conn, self.db_config.create_partition_sql(full_target_table_name, partitions, True))
            _exec_sql(self.conn, self.db_config.insert_data_sql(full_target_table_name, col_names,
                                                                f'select {col_names} from {source_table.get_full_table_name(self.temp_schema)}',
                                                                target_table.partitions))
        else:
            raise Exception(f'unknown save mode {save_mode}')

    def _ensure_contain_target_cols(self, source_cols: List[Dict], target_cols: List[Dict]):
        source_cols = [(col['name'],) for col in source_cols]
        target_cols = [(col['name'],) for col in target_cols]
        if not set(target_cols).issubset(set(source_cols)):
            raise Exception(
                f'source_cols does not contain target_cols: source_cols={source_cols}, target_cols={target_cols}')

    def create_table_with_data(self, full_table_name: str, values: List[List[Any]], schema: List[Col], partitions: List['Partition']):
        db, table = full_table_name[:full_table_name.index('.')], full_table_name[full_table_name.index('.') + 1:]
        _exec_sql(self.conn, self.db_config.create_db_sql(db))
        _exec_sql(self.conn, self.db_config.create_table_with_partitions_sql(full_table_name, [col.as_dict() for col in schema], partitions))
        pt_cols = [p.field for p in partitions]
        cols = [col.name for col in schema]
        pt_values_list = [[row[cols.index(p)] for p in pt_cols] for row in values]
        if partitions and not self.db_config.create_partition_automatically():
            partitions = set([tuple([Partition(field, value) for field, value in zip(pt_cols, pt_values)]) for pt_values in pt_values_list])
            for partition in partitions:
                _exec_sql(self.conn, self.db_config.create_partition_sql(full_table_name, list(partition)))
        from sqlalchemy.sql import text
        converted_col_names = ", ".join(self.db_config.convert_col([f":{col}" for col in cols], pt_cols))
        stmt = text(f'insert into {full_table_name} ({", ".join(cols)}) VALUES ({converted_col_names})')
        for v in values:
            _exec_sql(self.conn, stmt, **dict([(col, _v) for _v, col in zip(v, cols)]))

    def create_temp_table_with_data(self, table_name: str, values: List[List[Any]], schema: List[Col]):
        _exec_sql(self.conn, self.db_config.create_table_with_partitions_sql(table_name, [col.as_dict() for col in schema], []))
        cols = [col.name for col in schema]
        from sqlalchemy.sql import text
        stmt = text(f'insert into {table_name} ({", ".join(cols)}) VALUES ({", ".join([f":{col}" for col in cols])})')
        for v in values:
            _exec_sql(self.conn, stmt, **dict([(col, _v) for _v, col in zip(v, cols)]))
