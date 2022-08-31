from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict, List, Tuple, Union

from easy_sql.sql_processor.backend.sql_dialect import SqlDialect

if TYPE_CHECKING:
    from sqlalchemy.engine import ResultProxy

    from ..base import Partition

__all__ = ["PgSqlDialect"]

from ...common import SqlProcessorAssertionError


class PostgrePartition:
    def __init__(self, name: str, value: Union[int, str]):
        self.name = name
        self.value = value
        self.value_next = value + "_" if isinstance(value, str) else value + 1
        self.value_expr = f"'{value}'" if isinstance(value, str) else str(value)
        self.value_next_expr = f"'{self.value_next}'" if isinstance(self.value_next, str) else str(self.value_next)
        self.table_suffix = str(value).lower().replace("-", "_")

    @property
    def field_name(self):
        return self.name

    def partition_table_name(self, table_name: str):
        return f"{table_name}__{self.table_suffix}"


class PgSqlDialect(SqlDialect):
    def create_db_sql(self, db: str) -> str:
        return f"create schema if not exists {db}"

    def use_db_sql(self, db: str) -> str:
        return f"set search_path='{db}'"

    def drop_db_sql(self, db: str):
        return f"drop schema if exists {db} cascade"

    def rename_table_sql(self, from_table: str, to_table: str) -> str:
        pure_to_table = to_table if "." not in to_table else to_table[to_table.index(".") + 1 :]
        return f"alter table {from_table} rename to {pure_to_table}"

    def rename_table_db_sql(self, table_name: str, schema: str):
        return f"alter table {table_name} set schema {schema}"

    def rename_view_sql(self, from_table: str, to_table: str) -> str:
        pure_to_table = to_table if "." not in to_table else to_table[to_table.index(".") + 1 :]
        return f"alter view {from_table} rename to {pure_to_table}"

    def create_view_sql(self, table_name: str, select_sql: str) -> str:
        return f"create view {table_name} as {select_sql}"

    def drop_view_sql(self, table_name: str) -> str:
        return f"drop view {table_name} cascade"

    def get_tables_sql(self, db) -> str:
        return (
            f"select tablename FROM pg_catalog.pg_tables where schemaname='{db}' union "
            f"select viewname FROM pg_catalog.pg_views where schemaname='{db}'"
        )

    def get_dbs_sql(self) -> str:
        return "select schemaname from pg_catalog.pg_tables union select schemaname FROM pg_catalog.pg_views"

    def create_table_sql(self, table_name: str, select_sql: str) -> str:
        return f"create table {table_name} as {select_sql}"

    def support_native_partition(self) -> bool:
        return True

    def support_move_individual_partition(self) -> bool:
        return True

    def delete_partition_sql(self, table_name, partitions: List[Partition]) -> str:
        if len(partitions) > 1:
            raise SqlProcessorAssertionError(
                f"Only support exactly one partition column, found: {[str(p) for p in partitions]}"
            )
        assert partitions[0].value is not None
        partition = PostgrePartition(partitions[0].field, partitions[0].value)
        return f"drop table if exists {partition.partition_table_name(table_name)}"

    def native_partitions_sql(self, table_name: str) -> Tuple[str, Callable[[ResultProxy], List[str]]]:
        if "." not in table_name:
            raise SqlProcessorAssertionError(
                "table name must be of format {DB}.{TABLE} when query native partitions, found: " + table_name
            )

        db, table = table_name[: table_name.index(".")], table_name[table_name.index(".") + 1 :]
        sql = f"""
        SELECT pg_catalog.pg_get_partkeydef(pt_table.oid) as partition_key
        FROM pg_class pt_table
            JOIN pg_namespace nmsp_pt_table   ON nmsp_pt_table.oid  = pt_table.relnamespace
        WHERE nmsp_pt_table.nspname='{db}' and pt_table.relname='{table}'
        """
        return sql, self.extract_partition_cols

    def extract_partition_cols(self, native_partitions_sql_result: ResultProxy) -> List[str]:
        partition_values = [v[0] for v in native_partitions_sql_result.fetchall()]
        if not partition_values:
            raise Exception("no partition values found, table may not exist")
        partition_value = partition_values[0]
        if partition_value is None:
            return []
        partition_value: str = partition_value
        if not partition_value.upper().startswith("RANGE (") or not partition_value.endswith(")"):
            raise Exception("unable to parse partition: " + partition_value)
        return partition_value[len("RANGE (") : -1].split(",")

    def create_table_with_partitions_sql(self, table_name: str, cols: List[Dict], partitions: List[Partition]):
        if len(partitions) > 1:
            raise SqlProcessorAssertionError(
                f"Only support exactly one partition column, found: {[str(p) for p in partitions]}"
            )
        cols_expr = ",\n".join([f"{col['name']} {col['type']}" for col in cols])
        partition_expr = f"partition by range({partitions[0].field})" if len(partitions) == 1 else ""
        return f"create table {table_name} (\n{cols_expr}\n) {partition_expr}"

    def create_partition_automatically(self):
        return False

    def create_temp_table_schema_like_target_schema(self):
        return False

    def create_partition_sql(
        self, target_table_name: str, partitions: List[Partition], if_not_exists: bool = False
    ) -> str:
        if len(partitions) > 1:
            raise SqlProcessorAssertionError(
                f"Only support exactly one partition column, found: {[str(p) for p in partitions]}"
            )
        assert partitions[0].value is not None
        partition = PostgrePartition(partitions[0].field, partitions[0].value)
        partition_table_name = partition.partition_table_name(target_table_name)
        _if_not_exists = "if not exists" if if_not_exists else ""
        return (
            f"create table {_if_not_exists} {partition_table_name} partition of {target_table_name} "
            f"for values from ({partition.value_expr}) to ({partition.value_next_expr})"
        )

    def create_partitions_with_data_sqls(
        self, source_table_name: str, target_table_name: str, col_names: List[str], partitions: List[List[Partition]]
    ):
        col_names_expr = ", ".join(col_names)
        if not partitions:
            return [
                f"insert into {target_table_name}({col_names_expr}) select {col_names_expr} from {source_table_name}"
            ]
        sqls = []
        for partition in partitions:
            if len(partition) > 1:
                raise SqlProcessorAssertionError(
                    f"Only support exactly one partition column, found: {[str(p) for p in partitions]}"
                )
            assert partition[0].value is not None
            partition = PostgrePartition(partition[0].field, partition[0].value)
            partition_table_name = partition.partition_table_name(target_table_name)
            temp_table_name = f"{partition_table_name}__temp"
            sqls.append(f"drop table if exists {temp_table_name}")
            sqls.append(
                f"create table {temp_table_name} (like {target_table_name} including defaults including constraints)"
            )
            sqls.append(
                f"alter table {temp_table_name} add constraint p check ({partition.field_name} >="
                f" {partition.value_expr} and {partition.field_name} < {partition.value_next_expr})"
            )
            filter_expr = f"{partition.field_name} = {partition.value_expr}"
            sqls.append(
                f"insert into {temp_table_name}({col_names_expr}) select {col_names_expr} from"
                f" {source_table_name} where {filter_expr}"
            )
            sqls.append(f"drop table if exists {partition_table_name}")
            sqls.append(
                f'alter table {temp_table_name} rename to {partition_table_name[partition_table_name.index(".") + 1:]}'
            )
            sqls.append(
                f"alter table {target_table_name} attach partition {partition_table_name} "
                f"for values from ({partition.value_expr}) to ({partition.value_next_expr})"
            )
            sqls.append(f"alter table {partition_table_name} drop constraint p")
        return sqls

    def insert_data_sql(self, table_name: str, col_names_expr: str, select_sql: str, partitions: List[Partition]):
        return f"insert into {table_name}({col_names_expr}) {select_sql}"

    def move_data_sql(self, target_table: str, temp_table: str, partitions: List[Partition]) -> List[str]:
        sql = []
        for partition in partitions:
            assert partition.value is not None
            pg_partition = PostgrePartition(partition.field, partition.value)
            temp_partition_table_name = pg_partition.partition_table_name(temp_table)
            target_partition_table_name = pg_partition.partition_table_name(target_table)
            sql.append(self.drop_table_sql(target_partition_table_name))
            sql.append(f"alter table {temp_table} detach partition {temp_partition_table_name}")
            sql.append((self.rename_table_sql(temp_partition_table_name, target_partition_table_name)))
            sql.append(
                f"alter table {target_table} attach partition {target_partition_table_name} "
                f"for values from ({pg_partition.value_expr}) to ({pg_partition.value_next_expr})"
            )
        return sql

    def drop_table_sql(self, table: str):
        return f"drop table if exists {table}"

    def support_static_partition(self):
        return True

    def create_table_like_sql(self, target_table_name: str, source_table_name: str, partitions: List[Partition]) -> str:
        if len(partitions) == 0:
            return f"create table {target_table_name} (like {source_table_name} including constraints)"
        elif len(partitions) > 1:
            raise SqlProcessorAssertionError(
                f"Only support exactly one partition column, found: {[str(p) for p in partitions]}"
            )
        else:
            partition_expr = f"partition by range({partitions[0].field})"
            return f"create table {target_table_name} (like {source_table_name} including constraints) {partition_expr}"
