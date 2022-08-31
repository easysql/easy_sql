from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict, List, Tuple

from easy_sql.sql_processor.backend.sql_dialect import SqlDialect, SqlExpr

if TYPE_CHECKING:
    from sqlalchemy.engine import ResultProxy

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
        drop_pt_metadata = f"alter table {self.partitions_table_name} delete where db_name = '{db}'"
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

    def get_tables_sql(self, db: str) -> str:
        return f"show tables in {db}"

    def get_dbs_sql(self) -> str:
        return "show databases"

    def create_table_sql(self, table_name: str, select_sql: str):
        return f"create table {table_name} engine=MergeTree order by tuple() as {select_sql}"

    def support_native_partition(self) -> bool:
        return True

    def support_move_individual_partition(self) -> bool:
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
        pt_value_expr = []
        for pt in partitions:
            assert pt.value is not None
            pt_value_expr.append(self.sql_expr.for_value(pt.value))
        pt_expr = f'tuple({", ".join(pt_value_expr)})'
        drop_pt = f"alter table {table_name} drop partition {pt_expr}"

        return [drop_pt, drop_pt_metadata]

    def native_partitions_sql(self, table_name: str) -> Tuple[str, Callable[[ResultProxy], List[str]]]:
        return f"show create table {table_name}", self.extract_partition_cols

    def extract_partition_cols(self, native_partitions_sql_result: ResultProxy) -> List[str]:
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
            f"create table if not exists {table_name} (\n{cols_expr}\n)\nengine=MergeTree\n{partition_expr}\norder by"
            " tuple() settings allow_nullable_key=1;"
        )

    def create_temp_table_with_schema_sql(self, temp_table_name: str):
        return f'create table if not exists {temp_table_name} as {temp_table_name.split("__temp")[0]}'

    def create_partition_automatically(self):
        return True

    def create_temp_table_schema_as_target(self):
        return True

    def insert_data_sql(
        self, table_name: str, col_names_expr: str, select_sql: str, partitions: List[Partition]
    ) -> List[str]:
        insert_data_sql = f"insert into {table_name}({col_names_expr}) {select_sql}"
        self._check_no_none_in_partition_values(partitions)
        if len(partitions) != 0:
            self._check_partition_field_single(partitions)
            drop_pt_metadata_if_exist, insert_pt_metadata = self._generate_pt_metadata_sql(table_name, partitions)
            return [insert_data_sql, drop_pt_metadata_if_exist, insert_pt_metadata]
        return [insert_data_sql]

    def move_data_sql(self, target_table_name: str, temp_table_name: str, partitions: List[Partition]) -> List[str]:
        self._check_no_none_in_partition_values(partitions)
        assert len(partitions) != 0
        self._check_partition_field_single(partitions)
        drop_pt_metadata_if_exist, insert_pt_metadata = self._generate_pt_metadata_sql(target_table_name, partitions)
        # Assumption: partition movement requires the two tables to have
        # the same structure, partition key, engine family and storage policy,
        # need to upgrade local clickhouse version (> 21) to
        # support moving feature https://github.com/ClickHouse/ClickHouse/issues/14582
        move_partition_sqls = []
        for partition in partitions:
            move_partition_sqls.append(
                f"alter table {temp_table_name} move partition '{partition.value}' to table {target_table_name}"
            )
        move_data_sql = " ".join(move_partition_sqls)
        return [move_data_sql, drop_pt_metadata_if_exist, insert_pt_metadata]

    def drop_table_sql(self, table: str) -> List[str]:
        drop_table_sql = f"drop table if exists {table}"
        db, pure_table_name = split_table_name(table)
        drop_pt_metadata = (
            f"alter table {self.partitions_table_name} delete where db_name = '{db}' and table_name ="
            f" '{pure_table_name}'"
        )
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
            insert_pt_metadata = (
                f"insert into {self.partitions_table_name} values('{db}', '{pure_table_name}', '{partitions[0].value}',"
                " now());"
            )
            return insert_pt_metadata

    def create_table_like_sql(self, target_table_name: str, source_table_name: str, partitions: List[Partition]) -> str:
        return f"create table if not exists {target_table_name} as {source_table_name}"

    def _generate_pt_metadata_sql(self, table_name: str, partitions: List[Partition]) -> Tuple[str, str]:
        db, pure_table_name = split_table_name(table_name)
        drop_pt_metadata_if_exist = (
            f"alter table {self.partitions_table_name} delete "
            f"where db_name = '{db}' and table_name = '{pure_table_name}' "
            f"and partition_value = '{partitions[0].value}'"
        )
        insert_pt_metadata = self.insert_pt_metadata_sql(table_name, partitions)
        return drop_pt_metadata_if_exist, insert_pt_metadata

    def _check_no_none_in_partition_values(self, partitions: List[Partition]):
        if any([pt.value is None for pt in partitions]):
            raise SqlProcessorAssertionError(
                f"cannot insert data when partition value is None, partitions: {partitions}, there maybe some bug,"
                " please check"
            )

    def _check_partition_field_single(self, partitions: List[Partition]):
        if len(partitions) > 1:
            raise SqlProcessorAssertionError(
                "for now clickhouse backend only support table with single field partition"
            )
