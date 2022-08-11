from typing import Dict, Callable, List, Tuple

from easy_sql.sql_processor.backend.sql_dialect import SqlDialect, SqlExpr
from ..base import Partition

__all__ = ["ChSqlDialect"]

from ...common import SqlProcessorAssertionError


def split_table_name(table_name: str) -> Tuple[str, ...]:
    if len(table_name.split(".")) != 2:
        raise Exception(f"cannot split table name: {table_name}")
    return tuple(table_name.split("."))


class ChSqlDialect(SqlDialect):
    def __init__(self, sql_expr: SqlExpr, partitions_table_name: str):
        self.partitions_table_name = partitions_table_name
        super(ChSqlDialect, self).__init__(sql_expr)

    def create_db_sql(self, db: str):
        return f"create database if not exists {db}"

    def use_db_sql(self, db: str):
        return f"use {db}"

    def drop_db_sql(self, db: str) -> List[str]:
        drop_db = f"drop database if exists {db}"
        drop_pt_metadata = f"alter table {self.partitions_table_name} delete " f"where db_name = '{db}'"
        return [drop_db, drop_pt_metadata]

    def rename_table_sql(self, from_table: str, to_table: str) -> str:
        return f"rename table {from_table} to {to_table}"

    def rename_table_db_sql(self, table_name: str, schema: str):
        pure_table_name = table_name if "." not in table_name else table_name[table_name.index(".") + 1 :]
        return f"rename table {table_name} to {schema}.{pure_table_name}"

    def rename_view_sql(self, from_table: str, to_table: str) -> str:
        return self.rename_table_sql(from_table, to_table)

    def drop_view_sql(self, table: str) -> str:
        return f"drop table {table}"

    def create_view_sql(self, table_name: str, select_sql: str) -> str:
        return f"create view {table_name} as {select_sql}"

    def get_tables_sql(self, db: str):
        return f"show tables in {db}"

    def create_table_sql(self, table_name: str, select_sql: str):
        return f"create table {table_name} engine=MergeTree order by tuple() as {select_sql}"

    def support_native_partition(self) -> bool:
        return True

    def delete_partition_sql(self, table_name, partitions: List[Partition]) -> List[str]:
        if len(partitions) > 1:
            raise SqlProcessorAssertionError(
                f"Only support exactly one partition column, found: {[str(p) for p in partitions]}"
            )
        db, pure_table_name = split_table_name(table_name)

        drop_pt_metadata = (
            f"alter table {self.partitions_table_name} delete "
            f"where db_name = '{db}' and table_name = '{pure_table_name}' "
            f"and partition_value = '{partitions[0].value}'"
        )

        pt_expr = f'tuple({", ".join([self.sql_expr.for_value(pt.value) for pt in partitions])})'
        drop_pt = f"alter table {table_name} drop partition {pt_expr}"

        return [drop_pt, drop_pt_metadata]

    def native_partitions_sql(
        self, table_name: str
    ) -> Tuple[str, Callable[["sqlalchemy.engine.ResultProxy"], List[str]]]:
        return f"show create table {table_name}", self.extract_partition_cols

    def extract_partition_cols(self, native_partitions_sql_result: "sqlalchemy.engine.ResultProxy") -> List[str]:
        create_table_sql_lines = native_partitions_sql_result.fetchall()[0][0].split("\n")
        pt_cols = []
        for line in create_table_sql_lines:
            if line.startswith("PARTITION BY ("):
                pt_cols = [col.strip() for col in line[len("PARTITION BY (") : -1].split(",")]
                break
            elif line.startswith("PARTITION BY "):
                pt_cols = [line[len("PARTITION BY ") :].strip()]
                break
        return pt_cols

    def create_table_with_partitions_sql(self, table_name: str, cols: List[Dict], partitions: List[Partition]):
        cols_expr = ",\n".join([f"{col['name']} {col['type']}" for col in cols])
        if len(partitions) == 0:
            partition_expr = ""
        elif len(partitions) == 1:
            partition_expr = f"partition by {partitions[0].field}"
        else:
            partition_expr = f'partition by tuple({", ".join([pt.field for pt in partitions])})'
        return (
            f"create table if not exists "
            f"{table_name} (\n{cols_expr}\n)\nengine=MergeTree\n{partition_expr}\norder by tuple() settings allow_nullable_key=1;"
        )

    def create_partition_automatically(self):
        return True

    def insert_data_sql(
        self, table_name: str, col_names_expr: str, select_sql: str, partitions: List[Partition]
    ) -> List[str]:
        insert_date_sql = f"insert into {table_name}({col_names_expr}) {select_sql};"

        if any([pt.value is None for pt in partitions]):
            raise SqlProcessorAssertionError(
                f"cannot insert data when partition value is None, partitions: {partitions}, there maybe some bug, please check"
            )

        if len(partitions) != 0:
            if len(partitions) > 1:
                raise SqlProcessorAssertionError(
                    "for now clickhouse backend only support table with single field partition"
                )
            db, pure_table_name = split_table_name(table_name)
            drop_pt_metadata_if_exist = (
                f"alter table {self.partitions_table_name} delete "
                f"where db_name = '{db}' and table_name = '{pure_table_name}' "
                f"and partition_value = '{partitions[0].value}'"
            )
            insert_pt_metadata = self.insert_pt_metadata_sql(table_name, partitions)
            return [insert_date_sql, drop_pt_metadata_if_exist, insert_pt_metadata]
        return [insert_date_sql]

    def drop_table_sql(self, table: str) -> List[str]:
        drop_table_sql = f"drop table if exists {table}"
        db, pure_table_name = split_table_name(table)
        drop_pt_metadata = f"alter table {self.partitions_table_name} delete where db_name = '{db}' and table_name = '{pure_table_name}'"
        return [drop_table_sql, drop_pt_metadata]

    def support_static_partition(self):
        return False

    def create_pt_meta_table_sql(self, db: str) -> str:
        # As clickhouse has create partition table in RdbBackend, no need to create again
        return "select 1"

    def insert_pt_metadata_sql(self, table_name: str, partitions: List[Partition]) -> str:
        if len(partitions) == 0:
            return ""
        elif len(partitions) > 1:
            raise SqlProcessorAssertionError("clickhouse only supports single-column partitioning.")
        else:
            db, pure_table_name = tuple(table_name.split("."))
            insert_pt_metadata = f"insert into {self.partitions_table_name} values('{db}', '{pure_table_name}', '{partitions[0].value}', now());"
            return insert_pt_metadata
