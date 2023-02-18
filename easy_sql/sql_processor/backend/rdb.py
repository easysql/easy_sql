from __future__ import annotations

import re
import time
import traceback
from datetime import datetime
from random import random
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Union

from ...logger import logger
from ..common import SqlProcessorAssertionError
from .base import Backend, Partition, Row, SaveMode, Table, TableMeta
from .sql_dialect import SqlDialect, SqlExpr
from .sql_dialect.bigquery import BqSqlDialect
from .sql_dialect.clickhouse import ChSqlDialect
from .sql_dialect.postgres import PgSqlDialect

__all__ = ["RdbBackend", "SqlExpr"]

from ...udf import udfs
from .base import Col

if TYPE_CHECKING:
    from sqlalchemy.engine import ResultProxy
    from sqlalchemy.engine.base import Connection, Engine
    from sqlalchemy.engine.reflection import Inspector
    from sqlalchemy.sql.elements import TextClause
    from sqlalchemy.types import TypeEngine


class TimeLog:
    time_took_tpl = "time took: {time_took:.3f}s"

    def __init__(self, start_log: str, end_log_tpl: str):
        self.start_log = start_log
        self.end_log_tpl = end_log_tpl
        self.start_dt = None

    def __enter__(self):
        logger.info(self.start_log)
        self.start_dt = datetime.now()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            logger.error(f"Error found:\n{traceback.format_exc()}")
        assert self.start_dt is not None
        time_took = (datetime.now() - self.start_dt).total_seconds()
        logger.info(self.end_log_tpl.format(time_took=time_took))


def _exec_sql(conn: Connection, sql: Union[str, TextClause, List[str]], *args, **kwargs) -> ResultProxy:
    from sqlalchemy.sql.elements import TextClause

    quote_fstr = lambda str_to_quote: str(str_to_quote).replace("{", "{{").replace("}", "}}")
    start_msg = lambda sql: f"start to execute sql: {sql}, args={args}, kwargs={kwargs}"
    end_msg_tpl = lambda sql: f"end to execute sql({TimeLog.time_took_tpl}): {quote_fstr(sql)}"

    if isinstance(sql, (str, TextClause)):
        with TimeLog(start_msg(sql), end_msg_tpl(sql)):
            return conn.execute(sql, *args, **kwargs)  # type: ignore
    else:
        execute_result = None
        for each_sql in sql:
            each_sql = each_sql.strip()
            if each_sql:
                with TimeLog(start_msg(each_sql), end_msg_tpl(each_sql)):
                    execute_result = conn.execute(each_sql, *args, **kwargs)
        return execute_result  # type: ignore


def _quote_str(x):
    return f"'{x}'" if isinstance(x, str) else f"{x}"


class RdbTable(Table):
    def __init__(self, backend, sql: str, actions: Optional[List[Tuple]] = None):
        self.backend: RdbBackend = backend
        self.db_config: SqlDialect = backend.sql_dialect
        self.sql = sql
        self._exec_sql = lambda sql: _exec_sql(self.backend.conn, sql)
        self._actions = actions or []

        self._temp_table_time_prefix = lambda: f"t_{round(time.time() * 1000)}_{int(random() * 100000):04d}"
        self._is_simple_query = lambda sql: re.match(r"^select \* from [\w.]+$", sql)
        self._table_name_of_simple_query = lambda sql: re.match(r"select \* from ([\w.]+)", sql).group(1)  # type: ignore

    @staticmethod
    def from_table_meta(backend, table_meta: TableMeta):
        table = RdbTable(backend, f"select * from {table_meta.get_full_table_name(backend.temp_schema)}")
        if table_meta.has_partitions():
            for pt in table_meta.partitions:
                if pt.field not in table.field_names():
                    table = table.with_column(pt.field, backend.sql_expr.for_value(pt.value))
                else:
                    if pt.value is not None:
                        logger.warning(
                            f"partition column already exists in table {table_meta.table_name}, but right now we"
                            f" provided a new value {pt.value} for partition column {pt.field}. Will ignore it."
                        )
        return table

    def _execute_actions(self):
        assert self.backend.sql_dialect is not None
        for action in self._actions:
            if action[0] == "limit":
                count = action[1]
                with TimeLog(
                    f"start to execute action: {action}", f"end to execute action({TimeLog.time_took_tpl}): {action}"
                ):
                    prefix = f"{self._temp_table_time_prefix()}"
                    limit_result_table_name = f"{prefix}_limit_{count}"
                    if self._is_simple_query(self.sql):
                        temp_table_name = self._table_name_of_simple_query(self.sql)
                        self._exec_sql(
                            self.backend.sql_dialect.create_view_sql(
                                limit_result_table_name, f"select * from {temp_table_name} limit {count}"
                            )
                        )
                    else:
                        temp_table_name = f"{prefix}_limit_{count}_source"
                        self._exec_sql(self.backend.sql_dialect.create_view_sql(temp_table_name, self.sql))
                        self._exec_sql(
                            self.backend.sql_dialect.create_view_sql(
                                limit_result_table_name,
                                f"select * from {self.backend.temp_schema}.{temp_table_name} limit {count}",
                            )
                        )
                    self.sql = f"select * from {self.backend.temp_schema}.{limit_result_table_name}"
            elif action[0] == "newcol":
                name, value = action[1], action[2]
                with TimeLog(
                    f"start to execute action: {action}", f"end to execute action({TimeLog.time_took_tpl}): {action}"
                ):
                    prefix = self._temp_table_time_prefix()
                    # for pg: max table name chars allowed is 63, the max length is 55 for newcol_table_name
                    newcol_table_name = f"{prefix}_newcol_{name[:30]}"
                    if self._is_simple_query(self.sql):
                        temp_table_name = self._table_name_of_simple_query(self.sql)
                        field_names = self._field_names(temp_table_name)
                        select_sql = f'select {", ".join(field_names)}, {value} as {name} from {temp_table_name}'
                    else:
                        # for pg: max table name chars allowed is 63, the max length is 62 for newcol_table_name
                        temp_table_name = f"{prefix}_newcol_{name[:30]}_source"
                        self._exec_sql(self.db_config.create_view_sql(temp_table_name, self.sql))
                        field_names = self._field_names(f"{self.backend.temp_schema}.{temp_table_name}")
                        select_sql = (
                            f'select {", ".join(field_names)}, {value} as {name} from'
                            f" {self.backend.temp_schema}.{temp_table_name}"
                        )

                    self._exec_sql(self.db_config.create_view_sql(newcol_table_name, select_sql))
                    self.sql = f"select * from {self.backend.temp_schema}.{newcol_table_name}"
            else:
                raise SqlProcessorAssertionError(f"unsupported action: {action}")
        self._actions = []

    def is_empty(self) -> bool:
        return self.count() == 0

    def field_names(self) -> List[str]:
        self._execute_actions()
        if self._is_simple_query(self.sql):
            temp_table_name = self._table_name_of_simple_query(self.sql)
            return self._field_names(temp_table_name)
        prefix = f"{self._temp_table_time_prefix()}"
        field_names_result_table_name = f"{prefix}_field_names"
        assert self.backend.sql_dialect is not None
        self._exec_sql(self.backend.sql_dialect.create_view_sql(field_names_result_table_name, self.sql))
        return self._field_names(f"{self.backend.temp_schema}.{field_names_result_table_name}")

    def _field_names(self, table_name: str) -> List[str]:
        result: ResultProxy = self._exec_sql(f"select * from {table_name} limit 0")
        result.close()
        return list(result.keys())

    def first(self) -> RdbRow:
        all_action_are_limit = all([action[0] == "limit" for action in self._actions])
        if all_action_are_limit:
            min_limit = min([action[1] for action in self._actions]) if len(self._actions) > 0 else 1

            result: ResultProxy = self._exec_sql(self.sql)
            if min_limit <= 0:
                return RdbRow(list(result.keys()), ())
            with TimeLog(
                f"start to fetch first row: {self.sql}", f"end to fetch first row({TimeLog.time_took_tpl}): {self.sql}"
            ):
                row = result.first()
            return RdbRow(list(result.keys()), row)  # type: ignore

        self._execute_actions()
        result: ResultProxy = self._exec_sql(self.sql)
        with TimeLog(
            f"start to fetch first row: {self.sql}", f"end to fetch first row({TimeLog.time_took_tpl}): {self.sql}"
        ):
            row = result.first()
        return RdbRow(list(result.keys()), row)  # type: ignore

    def limit(self, count: int) -> RdbTable:
        return RdbTable(self.backend, self.sql, self._actions + [("limit", count)])

    def with_column(self, name: str, value: Any) -> RdbTable:
        return RdbTable(self.backend, self.sql, self._actions + [("newcol", name, value)])

    def collect(self) -> List[RdbRow]:
        return self._collect()

    def _collect(self, row_count: Optional[int] = None) -> List[RdbRow]:
        self._execute_actions()

        result: ResultProxy = self._exec_sql(self.sql)
        # collect at most 1000 rows for now
        max_rows = 1000 if row_count is None else row_count
        with TimeLog(
            f"start to fetch first row: {self.sql}", f"end to fetch first row({TimeLog.time_took_tpl}): {self.sql}"
        ):
            try:
                rows = result.fetchmany(max_rows)
            except Exception as e:
                print("result.fetchmany(max_rows) found error: ", e)
                if e.args[0] == "This result object does not return rows. It has been closed automatically.":
                    return []
                else:
                    raise e
        if row_count is None and len(rows) == max_rows:
            logger.warning(
                f"found {max_rows} items, but there may be more, will only fetch {max_rows} items at most for sql:"
                f" {self.sql}"
            )
        rows = [RdbRow(list(result.keys()), row) for row in rows]  # type: ignore
        result.close()
        return rows

    def show(self, count: int = 20):
        keys = self.field_names()
        rows = self._collect(count)
        print("\t".join(keys))
        for row in rows:
            print("\t".join([_quote_str(item) for item in row]))

    def count(self) -> int:
        temp_table_name = self.resolve_to_temp_table()
        return self._exec_sql(f"select count(1) from {self.backend.temp_schema}.{temp_table_name}").first()[0]  # type: ignore

    def resolve_to_temp_table(self) -> str:
        self._execute_actions()
        if self._is_simple_query(self.sql) and "." not in self._table_name_of_simple_query(self.sql):
            temp_table_name = self._table_name_of_simple_query(self.sql)
        else:
            prefix = self._temp_table_time_prefix()
            temp_table_name = f"{prefix}_count"
            self._exec_sql(self.db_config.create_view_sql(temp_table_name, self.sql))
        self.sql = f"select * from {self.backend.temp_schema}.{temp_table_name}"
        return temp_table_name

    def save_to_temp_table(self, name: str):
        temp_table_name = self.resolve_to_temp_table()
        if temp_table_name != name:
            if "." in name:
                raise SqlProcessorAssertionError(
                    "renaming should only happen in temp database, "
                    f"so name must be pure TABLE_NAME when renaming tables, found: {name}"
                )
            if temp_table_name != name:
                if self.backend.table_exists(TableMeta(name)):
                    raise SqlProcessorAssertionError(
                        "we are trying to replace an existing temp table, it is not supported right now. table name:"
                        f" {name}"
                    )
                self._exec_sql(
                    self.db_config.create_view_sql(name, f"select * from {self.backend.temp_schema}.{temp_table_name}")
                )

    def save_to_table(self, target_table: TableMeta):
        temp_table_name = self.resolve_to_temp_table()

        field_names = self.field_names()
        for pt in target_table.partitions:
            if pt.field not in field_names:
                raise Exception(
                    f"does not found partition field `{pt.field}` in source table for target table"
                    f" {target_table.table_name}, all fields are in source table: {field_names}"
                )

        cols = self.backend.get_columns(temp_table_name, self.backend.temp_schema)
        if self.backend.table_exists(target_table):
            cols = self.backend.get_columns(target_table.pure_table_name, target_table.dbname)
        else:
            db = target_table.table_name[: target_table.table_name.index(".")]
            self._exec_sql(self.db_config.create_db_sql(db))
            self._exec_sql(
                self.db_config.create_table_with_partitions_sql(target_table.table_name, cols, target_table.partitions)
            )

        target_table_name = target_table.get_full_table_name(self.backend.temp_schema)

        partitions_to_save = self._get_save_partitions(target_table, temp_table_name)

        if not self.db_config.create_partition_automatically():
            source_table_name = f"{self.backend.temp_schema}.{temp_table_name}"
            sqls = self.db_config.create_partitions_with_data_sqls(
                source_table_name, target_table_name, [col["name"] for col in cols], partitions_to_save
            )
            for sql in sqls:
                self._exec_sql(sql)
        else:
            cols = [col["name"] for col in cols]
            col_names = ", ".join(cols)
            converted_col_names = ", ".join(
                self.db_config.convert_pt_col_expr(cols, [pt.field for pt in target_table.partitions])
            )
            if not partitions_to_save:
                self._exec_sql(
                    self.db_config.insert_data_sql(
                        target_table_name,
                        col_names,
                        f"select {converted_col_names} from {self.backend.temp_schema}.{temp_table_name}",
                        [],
                    )
                )
            for partitions in partitions_to_save:
                filter_expr = " and ".join(
                    [f"{pt.field} = {self.backend.sql_expr.for_value(pt.value)}" for pt in partitions]  # type: ignore
                )
                self._exec_sql(
                    self.db_config.insert_data_sql(
                        target_table_name,
                        col_names,
                        (
                            f"select {converted_col_names} from {self.backend.temp_schema}.{temp_table_name} where"
                            f" {filter_expr}"
                        ),
                        partitions,
                    )
                )

    def _get_save_partitions(self, target_table, temp_table_name):
        partitions_to_save = [target_table.partitions] if target_table.partitions else []
        if target_table.has_dynamic_partition():
            partition_values = self.backend.exec_sql(
                f'select distinct {", ".join([p.field for p in target_table.partitions])} '
                f"from {self.backend.temp_schema}.{temp_table_name}"
            ).collect()
            partitions_to_save = [
                [Partition(p.field, v[i]) for i, p in enumerate(target_table.partitions)] for v in partition_values
            ]
        return partitions_to_save


class RdbRow(Row):
    def __init__(self, cols: List[str], values: Tuple):
        self._cols = cols
        from decimal import Decimal

        # case decimal to float in order for later comparing (to ensure type consistency)
        self._values = tuple([float(v) if isinstance(v, Decimal) else v for v in values])

    def as_dict(self):
        return None if self._values is None else dict(zip(self._cols, self._values))

    def as_tuple(self):
        return self._values

    def __eq__(self, other: RdbRow):
        if (
            not isinstance(
                other,
                (
                    RdbRow,
                    tuple,
                ),
            )
            or other is None
        ):
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
        return "RdbRow" + str(self)


class RdbBackend(Backend):
    """table_partitions_table_name;
    means the table name which save the static partition info for all partition tables in data warehouse,
    for now need support backend type: [clickhouse]
    others backend has another method to manage static partition info or just support static partition"""

    def __init__(
        self,
        url: str,
        credentials: Optional[str] = None,
        sql_expr: Optional[SqlExpr] = None,
        partitions_table_name="dataplat.__table_partitions__",
        engine: Optional[Engine] = None,
    ):
        self.partitions_table_name = partitions_table_name
        self.url, self.credentials = url, credentials
        self.sql_expr = sql_expr or SqlExpr()
        self.__init_inner(self.url, self.credentials, engine)

    def __init_inner(self, url: str, credentials: Optional[str] = None, engine: Optional[Engine] = None):
        from sqlalchemy import create_engine

        self.temp_schema = f"sp_temp_{int(time.mktime(time.gmtime()))}_{int(random() * 10000):04d}"

        self.is_pg, self.is_ch, self.is_bq = False, False, False
        if engine:
            self.engine = engine
        elif url.startswith("postgresql://"):
            self.backend_type, self.is_pg = "pg", True
            self.sql_dialect = PgSqlDialect(self.sql_expr)
            self.engine: Engine = create_engine(url, isolation_level="AUTOCOMMIT", pool_size=1)
            self.conn: Connection = self.engine.connect()
            _exec_sql(self.conn, self.sql_dialect.create_db_sql(self.temp_schema))
            _exec_sql(self.conn, self.sql_dialect.use_db_sql(self.temp_schema))
        elif url.startswith("clickhouse://") or url.startswith("clickhouse+native://"):
            self.backend_type, self.is_ch = "ch", True
            self.sql_dialect = ChSqlDialect(self.sql_expr, self.partitions_table_name)

            _engine: Engine = create_engine(url, pool_size=1)
            conn: Connection = _engine.connect()
            _exec_sql(conn, self.sql_dialect.create_db_sql(self.temp_schema))

            self._create_partitions_table(conn)

            conn.close()

            url_parts = url.split("?")
            url_params = "" if len(url_parts) == 1 else f"?{url_parts[1]}"
            url_raw_parts = url_parts[0].split("/")
            if len(url_raw_parts) == 4:  # db in url
                url_raw_parts = url_raw_parts[:-1]
            elif len(url_raw_parts) == 3:  # db not in url
                pass
            else:
                raise Exception(f"unrecognized url: {url}")
            url = f'{"/".join(url_raw_parts + [self.temp_schema])}{url_params}'

            self.engine: Engine = create_engine(url, pool_size=1)
            self.conn: Connection = self.engine.connect()
        elif url.startswith("bigquery://"):
            self.backend_type, self.is_bq = "bq", True
            self.sql_dialect = BqSqlDialect(self.temp_schema, self.sql_expr)
            self.engine: Engine = create_engine(url, credentials_path=credentials)
            self.conn: Connection = self.engine.connect()
            _exec_sql(self.conn, self.sql_dialect.create_db_sql(self.temp_schema))
        else:
            raise Exception(f"unsupported url: {url}")

    def _create_partitions_table(self, conn):
        cols = [
            {"name": "db_name", "type": "String"},
            {"name": "table_name", "type": "String"},
            {"name": "partition_value", "type": "String"},
            {"name": "last_modified_time", "type": "DateTime"},
        ]
        partitions = [Partition(field="db_name")]
        db_name = self.partitions_table_name.split(".")[0]
        assert self.sql_dialect is not None
        _exec_sql(conn, self.sql_dialect.create_db_sql(db_name))
        _exec_sql(
            conn,
            self.sql_dialect.create_table_with_partitions_sql(self.partitions_table_name, cols, partitions),  # type: ignore
        )

    @property
    def inspector(self) -> Inspector:
        from sqlalchemy import inspect

        # inspector object has cache built-in, so we should recreate the object if required
        inspector: Inspector = inspect(self.engine)
        return inspector

    def get_columns(self, table_name, schema=None, raw=False, **kw) -> List[Dict]:
        cols = self.inspector.get_columns(table_name, schema, **kw)
        if not raw:
            for col in cols:
                col_type: TypeEngine = col["type"]
                col["type"] = col_type.compile(self.inspector.dialect)  # type: ignore
        return cols

    def get_column_names(self, table_name, schema=None, **kw) -> List[str]:
        cols = self.get_columns(table_name, schema, raw=True, **kw)
        names = [col["name"] for col in cols if "name" in col]
        return names

    def reset(self):
        if self.conn:
            try:  # noqa: SIM105
                self.conn.close()
            except Exception:
                pass
        if self.engine:
            try:  # noqa: SIM105
                self.engine.dispose()
            except Exception:
                pass
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
        return RdbTable(self, "")

    def exec_native_sql(self, sql: Union[str, List[str]]) -> Any:
        return _exec_sql(self.conn, sql)

    def exec_sql(self, sql: str) -> RdbTable:
        return RdbTable(self, sql)

    def _tables(self, db: str) -> List[str]:
        all_tables = _exec_sql(self.conn, self.sql_dialect.get_tables_sql(db)).fetchall()
        return sorted([table[0] for table in all_tables])

    def _dbs(self) -> List[str]:
        all_schemas = _exec_sql(self.conn, self.sql_dialect.get_dbs_sql()).fetchall()
        return sorted([schema[0] for schema in all_schemas])

    def temp_tables(self) -> List[str]:
        return self._tables(self.temp_schema)

    def clear_cache(self):
        pass

    def clean(self):
        logger.info(f"clean temp db: {self.temp_schema}")
        _exec_sql(self.conn, self.sql_dialect.drop_db_sql(self.temp_schema))

    def clear_temp_tables(self, exclude: Optional[List[str]] = None):
        from sqlalchemy.exc import ProgrammingError

        exclude = exclude or []
        for table in self.temp_tables():
            if table not in exclude:
                print(f"dropping temp table {table}")
                try:
                    _exec_sql(self.conn, self.sql_dialect.drop_view_sql(table))
                except ProgrammingError as e:
                    if re.match(r'.*view ".*" does not exist', e.args[0]):
                        # Since we will drop view cascade in pg, so some view might already be dropped.
                        # It will raise the view-not-exist error, we just ignore this kind of error.
                        pass
                    else:
                        raise e

    def create_temp_table(self, table: RdbTable, name: str):
        logger.info(f"create_temp_table with: table={table}, name={name}")
        table.save_to_temp_table(name)

    def create_cache_table(self, table: RdbTable, name: str):
        logger.info(f"create_cache_table with: table={table}, name={name}")
        table.save_to_temp_table(name)

    def broadcast_table(self, table: RdbTable, name: str):
        logger.info(f"broadcast_table with: table={table}, name={name}")
        table.save_to_temp_table(name)

    def db_exists(self, db_name: str) -> bool:
        return db_name in self._dbs()

    def table_exists(self, table: TableMeta):
        db_name, table_name = table.dbname, table.pure_table_name
        db_name = db_name or self.temp_schema
        return self.db_exists(db_name) and table_name in self._tables(db_name)

    def refresh_table_partitions(self, table: TableMeta):
        if self.sql_dialect.support_native_partition():
            native_partitions_sql, extract_partition_cols = self.sql_dialect.native_partitions_sql(table.table_name)
            pt_cols = extract_partition_cols(_exec_sql(self.conn, native_partitions_sql))
            table.update_partitions([Partition(col) for col in pt_cols])
        # no need to do anything, if the db does not support partition

    def _get_save_partitions(self, original_source_table, source_table, target_table):
        # if original_source_table has dynamic partitions , it will generate multi partitions
        # if original_source_table has static partitions, it will generate only one partition which is target_table.partitions
        save_partitions_list = []
        if original_source_table.has_dynamic_partition():  # dynamic partitions (partition retrieved from real table)
            pt_cols = [pt.field for pt in original_source_table.partitions]
            pt_values_list = _exec_sql(
                self.conn,
                f'select distinct {", ".join(pt_cols)} from {source_table.get_full_table_name(self.temp_schema)}',
            ).fetchall()
            for pt_values in pt_values_list:
                save_partitions_list.append([Partition(field, value) for field, value in zip(pt_cols, pt_values)])
        else:  # static partitions (partition specified in sql file)
            save_partitions_list.append(target_table.partitions)
        return save_partitions_list

    def save_table_sql(self, source_table: TableMeta, source_table_sql: str, target_table: TableMeta) -> str:
        source_col_names = self.get_column_names(source_table.pure_table_name, source_table.dbname or self.temp_schema)
        return f'insert into {target_table.table_name} select {",".join(source_col_names)} from ({source_table_sql})'

    def save_table(
        self, source_table: TableMeta, target_table: TableMeta, save_mode: SaveMode, create_target_table: bool
    ):
        logger.info(
            f"save table with: source_table={source_table}, target_table={target_table}, "
            f"save_mode={save_mode}, create_target_table={create_target_table}"
        )

        if not self.sql_dialect.support_static_partition():
            assert target_table.dbname is not None
            if not self.db_exists(target_table.dbname) and create_target_table:
                _exec_sql(self.conn, self.sql_dialect.create_db_sql(target_table.dbname))
            _exec_sql(self.conn, self.sql_dialect.create_pt_meta_table_sql(target_table.dbname))

        if not self.table_exists(target_table) and not create_target_table:
            raise Exception(
                f"target table {target_table.table_name} does not exist, and create_target_table is False, "
                f"cannot save table {source_table.table_name} to {target_table.table_name}"
            )

        source_table = source_table.clone_with_partitions(target_table.partitions)
        if not self.table_exists(target_table) and create_target_table:
            RdbTable.from_table_meta(self, source_table).save_to_table(target_table)
            return

        target_col_names = self.get_column_names(target_table.pure_table_name, target_table.dbname or self.temp_schema)
        original_source_table = source_table
        source_table = TableMeta(RdbTable.from_table_meta(self, source_table).resolve_to_temp_table())
        source_col_names = self.get_column_names(source_table.pure_table_name, source_table.dbname or self.temp_schema)
        logger.info(
            f"ensure cols match for source_table {source_table.table_name} and target_table {target_table.table_name}"
        )
        self._ensure_contain_target_cols(source_col_names, target_col_names)

        if save_mode == SaveMode.overwrite:
            self._overwrite(source_table, target_table, original_source_table, target_col_names)
        elif save_mode == SaveMode.append:
            self._append(source_table, target_table, original_source_table, target_col_names)
        else:
            raise SqlProcessorAssertionError(f"unknown save mode {save_mode}")

    def _ensure_contain_target_cols(self, source_cols: List[str], target_cols: List[str]):
        if not set(target_cols).issubset(set(source_cols)):
            raise Exception(
                f"source_cols does not contain target_cols: source_cols={source_cols}, target_cols={target_cols}"
            )

    def create_table_with_data(
        self, full_table_name: str, values: List[List[Any]], schema: List[Col], partitions: List[Partition]
    ):
        db, _ = full_table_name[: full_table_name.index(".")], full_table_name[full_table_name.index(".") + 1 :]
        _exec_sql(self.conn, self.sql_dialect.create_db_sql(db))
        _exec_sql(
            self.conn,
            self.sql_dialect.create_table_with_partitions_sql(
                full_table_name, [col.as_dict() for col in schema], partitions  # type: ignore
            ),
        )
        cols = [col.name for col in schema]
        pt_cols = [p.field for p in partitions]
        pt_values_list = [[row[cols.index(p)] for p in pt_cols] for row in values]
        partitions_with_values = {
            tuple([Partition(field, value) for field, value in zip(pt_cols, pt_values)]) for pt_values in pt_values_list
        }
        if partitions_with_values and not self.sql_dialect.create_partition_automatically():
            for partition in partitions_with_values:
                if partition:
                    _exec_sql(self.conn, self.sql_dialect.create_partition_sql(full_table_name, list(partition)))
        from sqlalchemy.sql import text

        converted_col_names = ", ".join(self.sql_dialect.convert_pt_col_expr([f":{col}" for col in cols], pt_cols))
        stmt = text(f'insert into {full_table_name} ({", ".join(cols)}) VALUES ({converted_col_names})')
        for v in values:
            _exec_sql(self.conn, stmt, **{col: _v for _v, col in zip(v, cols)})

        if partitions_with_values and not self.sql_dialect.support_static_partition():
            _exec_sql(self.conn, self.sql_dialect.create_pt_meta_table_sql(db))
            for partition in partitions_with_values:
                if partition:
                    _exec_sql(self.conn, self.sql_dialect.insert_pt_metadata_sql(full_table_name, list(partition)))

    def create_temp_table_with_data(self, table_name: str, values: List[List[Any]], schema: List[Col]):
        _exec_sql(
            self.conn,
            self.sql_dialect.create_table_with_partitions_sql(table_name, [col.as_dict() for col in schema], []),  # type: ignore
        )
        cols = [col.name for col in schema]
        from sqlalchemy.sql import text

        stmt = text(f'insert into {table_name} ({", ".join(cols)}) VALUES ({", ".join([f":{col}" for col in cols])})')
        for v in values:
            _exec_sql(self.conn, stmt, **{col: _v for _v, col in zip(v, cols)})

    def _overwrite(
        self,
        source_table: TableMeta,
        target_table: TableMeta,
        original_source_table: TableMeta,
        target_cols: List[str],
    ):
        col_names = ", ".join(target_cols)
        full_target_table_name = target_table.get_full_table_name(self.temp_schema)
        # write data to temp table to support the case when read from and write to the same table
        temp_table_name = f"{full_target_table_name}__temp"
        _exec_sql(self.conn, self.sql_dialect.drop_table_sql(temp_table_name))
        if self.table_exists(target_table):
            _exec_sql(
                self.conn,
                self.sql_dialect.create_table_like_sql(
                    temp_table_name, target_table.table_name, target_table.partitions
                ),
            )
        RdbTable.from_table_meta(self, source_table).save_to_table(target_table.clone_with_name(temp_table_name))
        if original_source_table.has_partitions():
            save_partitions = self._get_save_partitions(original_source_table, source_table, target_table)
            for save_partition in save_partitions:
                _exec_sql(self.conn, self.sql_dialect.delete_partition_sql(target_table.table_name, save_partition))
                if not self.sql_dialect.create_partition_automatically():
                    _exec_sql(self.conn, self.sql_dialect.create_partition_sql(full_target_table_name, save_partition))

                if self.sql_dialect.support_move_individual_partition():
                    _exec_sql(
                        self.conn,
                        self.sql_dialect.move_data_sql(full_target_table_name, temp_table_name, save_partition),
                    )
                else:
                    filter_expr = " and ".join(
                        [f"{pt.field} = {self.sql_expr.for_value(pt.value)}" for pt in save_partition]
                    )
                    _exec_sql(
                        self.conn,
                        self.sql_dialect.insert_data_sql(
                            full_target_table_name,
                            col_names,
                            f"select {col_names} from {temp_table_name} where {filter_expr}",
                            save_partition,
                        ),
                    )
            _exec_sql(self.conn, self.sql_dialect.drop_table_sql(temp_table_name))
        else:
            _exec_sql(self.conn, self.sql_dialect.drop_table_sql(full_target_table_name))
            _exec_sql(self.conn, self.sql_dialect.rename_table_sql(temp_table_name, full_target_table_name))

    def _append(
        self,
        source_table: TableMeta,
        target_table: TableMeta,
        original_source_table: TableMeta,
        target_cols: List[str],
    ) -> None:
        col_names = ", ".join(target_cols)
        full_target_table_name = target_table.get_full_table_name(self.temp_schema)
        if original_source_table.has_partitions():
            save_partitions = self._get_save_partitions(original_source_table, source_table, target_table)
            for save_partition in save_partitions:
                if not self.sql_dialect.create_partition_automatically():
                    _exec_sql(
                        self.conn,
                        self.sql_dialect.create_partition_sql(full_target_table_name, save_partition, True),
                    )
                filter_expr = " and ".join(
                    [f"{pt.field} = {self.sql_expr.for_value(pt.value)}" for pt in save_partition]
                )
                _exec_sql(
                    self.conn,
                    self.sql_dialect.insert_data_sql(
                        full_target_table_name,
                        col_names,
                        (
                            f"select {col_names} from {source_table.get_full_table_name(self.temp_schema)} where"
                            f" {filter_expr}"
                        ),
                        save_partition,
                    ),
                )
        else:
            _exec_sql(
                self.conn,
                self.sql_dialect.insert_data_sql(
                    full_target_table_name,
                    col_names,
                    f"select {col_names} from {source_table.get_full_table_name(self.temp_schema)}",
                    [],
                ),
            )
