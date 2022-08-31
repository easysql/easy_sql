from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict, List, Tuple

from easy_sql.sql_processor.backend.sql_dialect import SqlDialect, SqlExpr

if TYPE_CHECKING:
    from sqlalchemy.engine import ResultProxy
    from sqlalchemy.types import TypeEngine

    from ..base import Partition

__all__ = ["BqSqlDialect"]

from ...common import SqlProcessorAssertionError


class BqSqlDialect(SqlDialect):
    def __init__(self, db: str, sql_expr: SqlExpr):
        self.db = db
        super().__init__(sql_expr)

    def create_db_sql(self, db: str) -> str:
        return f"create schema if not exists {db}"

    # There's no such equivalent statement like 'use ${db}' in BigQuery.
    # Table must be qualified with a dataset when using BigQuery
    def use_db_sql(self, db: str) -> str:
        return "select 1"

    def drop_db_sql(self, db: str) -> str:
        return f"drop schema if exists {db} cascade"

    def rename_table_sql(self, from_table: str, to_table: str) -> str:
        from_full_table_name = from_table if self.contains_db(from_table) else f"{self.db}.{from_table}"
        to_pure_table_name = to_table.split(".")[1] if self.contains_db(to_table) else to_table
        return f"alter table if exists {from_full_table_name} rename to {to_pure_table_name}"

    # There's no statement that could change the dataset directly
    # Copy cannot be operated at view
    def rename_table_db_sql(self, table_name: str, schema: str):
        pure_table_name = table_name[table_name.index(".") + 1 :] if self.contains_db(table_name) else table_name
        from_table_name = table_name if self.contains_db(table_name) else f"{self.db}.{table_name}"
        return f"create table if not exists {schema}.{pure_table_name} copy {from_table_name}"

    def get_tables_sql(self, db) -> str:
        return f"select table_name from {db}.INFORMATION_SCHEMA.TABLES"

    def get_dbs_sql(self) -> str:
        return "select schema_name from INFORMATION_SCHEMA.SCHEMATA"

    def create_table_sql(self, table_name: str, select_sql: str) -> str:
        full_table_name = table_name if self.contains_db(table_name) else f"{self.db}.{table_name}"
        return f"create table if not exists {full_table_name} as {select_sql}"

    # Cannot rename view directly in BigQuery
    def rename_view_sql(self, from_table: str, to_table: str) -> str:
        from_table_name = from_table if self.contains_db(from_table) else f"{self.db}.{from_table}"
        to_table_name = to_table if self.contains_db(to_table) else f"{self.db}.{to_table}"
        return f"create view if not exists {to_table_name} as select * from {from_table_name}"

    def drop_view_sql(self, table: str) -> str:
        return f"drop view if exists {self.db}.{table}"

    def create_view_sql(self, table_name: str, select_sql: str) -> str:
        full_table_name = table_name if self.contains_db(table_name) else f"{self.db}.{table_name}"
        return f"create view if not exists {full_table_name} as {select_sql}"

    def support_native_partition(self) -> bool:
        return True

    def delete_partition_sql(self, table_name, partitions: List[Partition]) -> str:
        if not self.contains_db(table_name):
            raise SqlProcessorAssertionError("BigQuery table must be qualified with a dataset.")

        db, pure_table_name = tuple(table_name.split("."))
        if not partitions or len(partitions) == 0:
            raise Exception(
                f"cannot delete partition when partitions not specified: table_name={table_name},"
                f" partitions={partitions}"
            )
        if len(partitions) > 1:
            raise SqlProcessorAssertionError("BigQuery only supports single-column partitioning.")
        pt_expr = f"""{partitions[0].field} = '{partitions[0].value}'"""

        delete_pt_sql = f"delete {db}.{pure_table_name} where {pt_expr};"
        delete_pt_metadata = (
            f"delete {db}.__table_partitions__ where table_name = '{pure_table_name}' and partition_value ="
            f" '{partitions[0].value}';"
        )
        return self.transaction(f"{delete_pt_sql}\n{delete_pt_metadata}")

    def native_partitions_sql(self, table_name: str) -> Tuple[str, Callable[[ResultProxy], List[str]]]:
        db = table_name.split(".")[0] if self.contains_db(table_name) else {self.db}
        pure_table_name = table_name.split(".")[1] if self.contains_db(table_name) else {table_name}
        return (
            f"select ddl from {db}.INFORMATION_SCHEMA.TABLES where table_name = '{pure_table_name}'",
            self.extract_partition_cols,
        )

    def extract_partition_cols(self, native_partitions_sql_result):
        create_table_sql_lines = native_partitions_sql_result.fetchall()[0][0].split("\n")
        pt_cols = []
        for line in create_table_sql_lines:
            if line.startswith("PARTITION BY "):
                pt_cols = [line[len("PARTITION BY ") : -1].strip()]
                break
        return pt_cols

    def create_table_with_partitions_sql(
        self, table_name: str, cols: List[Dict[str, TypeEngine]], partitions: List[Partition]
    ):
        cols_expr = ",\n".join(
            f"{col['name']} {self.sql_expr.for_bigquery_type(col['name'], col['type'])}" for col in cols  # type: ignore
        )
        if len(partitions) == 0:
            partition_expr = ""
        elif len(partitions) == 1:
            partition_expr = f"partition by {self.sql_expr.bigquery_partition_expr(partitions[0].field)}"
        else:
            raise SqlProcessorAssertionError("BigQuery only supports single-column partitioning.")
        table_name_with_db = table_name if self.contains_db(table_name) else f"{self.db}.{table_name}"
        return f"create table if not exists {table_name_with_db} (\n{cols_expr}\n)\n{partition_expr}\n"

    def create_partition_automatically(self):
        return True

    def insert_data_sql(self, table_name: str, col_names_expr: str, select_sql: str, partitions: List[Partition]):
        if not self.contains_db(table_name):
            raise SqlProcessorAssertionError("BigQuery table must be qualified with a dataset.")
        if any([pt.value is None for pt in partitions]):
            raise SqlProcessorAssertionError(
                f"cannot insert data when partition value is None, partitions: {partitions}, "
                "there maybe some bug, please check"
            )

        insert_date_sql = f"insert into {table_name}({col_names_expr}) {select_sql};"
        delete_pt_metadata_if_exist = self.delete_pt_metadata_sql(table_name, partitions)
        insert_pt_metadata = self.insert_pt_metadata_sql(table_name, partitions)
        return self.transaction(f"{insert_date_sql}\n{delete_pt_metadata_if_exist}\n{insert_pt_metadata}")

    def drop_table_sql(self, table: str):
        if not self.contains_db(table):
            raise SqlProcessorAssertionError("BigQuery table must be qualified with a dataset.")
        db, pure_table_name = tuple(table.split("."))
        drop_table_sql = f"drop table if exists {db}.{pure_table_name};"
        drop_pt_metadata = f"delete {db}.__table_partitions__ where table_name = '{pure_table_name}';"
        return f"{drop_table_sql}\n{drop_pt_metadata}"

    def support_static_partition(self):
        return False

    def support_move_individual_partition(self):
        return False

    def create_pt_meta_table_sql(self, db: str) -> str:
        return f"""create table if not exists {db}.__table_partitions__(
        table_name string, partition_value string, last_modified_time timestamp)
        cluster by table_name;"""

    def insert_pt_metadata_sql(self, table_name: str, partitions: List[Partition]) -> str:
        if len(partitions) == 0:
            return ""
        elif len(partitions) > 1:
            raise SqlProcessorAssertionError("BigQuery only supports single-column partitioning.")
        else:
            if not self.contains_db(table_name):
                raise SqlProcessorAssertionError("BigQuery table must be qualified with a dataset.")
            db, pure_table_name = tuple(table_name.split("."))
            return (
                f"insert into {db}.__table_partitions__ values ('{pure_table_name}', '{partitions[0].value}',"
                " CURRENT_TIMESTAMP());"
            )

    def delete_pt_metadata_sql(self, table_name: str, partitions: List[Partition]) -> str:
        if len(partitions) == 0:
            return ""
        elif len(partitions) > 1:
            raise SqlProcessorAssertionError("BigQuery only supports single-column partitioning.")
        else:
            if not self.contains_db(table_name):
                raise SqlProcessorAssertionError("BigQuery table must be qualified with a dataset.")
            db, pure_table_name = tuple(table_name.split("."))
            return (
                f"delete {db}.__table_partitions__ where table_name = '{pure_table_name}' and partition_value ="
                f" '{partitions[0].value}';"
            )

    def create_table_like_sql(self, target_table_name: str, source_table_name: str, partitions: List[Partition]) -> str:
        return f"create table {target_table_name} like {source_table_name}"

    @staticmethod
    def transaction(statement: str) -> str:
        return f"BEGIN TRANSACTION;\n{statement}\nCOMMIT TRANSACTION;"

    @staticmethod
    def contains_db(table_name: str) -> bool:
        return "." in table_name
