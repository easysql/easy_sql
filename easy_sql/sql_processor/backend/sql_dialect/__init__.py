from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Union

if TYPE_CHECKING:
    from sqlalchemy.engine import ResultProxy
    from sqlalchemy.types import TypeEngine

    from ..base import Partition

__all__ = ["SqlExpr", "SqlDialect"]

from ...common import SqlProcessorAssertionError


class SqlExpr:
    def __init__(
        self,
        value_to_sql_expr: Optional[Callable[[Any], Optional[str]]] = None,
        column_sql_type_converter: Optional[Callable[[str, str, TypeEngine], Optional[str]]] = None,
        partition_col_converter: Optional[Callable[[str], str]] = None,
        partition_value_converter: Optional[Callable[[str, str], Any]] = None,
        partition_expr: Optional[Callable[[str, str], str]] = None,
    ):
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
            return self.partition_expr("bigquery", partition_col)
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
            raise SqlProcessorAssertionError(
                "when create new columns, the current supported value types are [str, int, float, datetime, date],"
                f" found: value={value}, type={type(value)}"
            )
        if isinstance(value, (str,)):
            return f"'{value}'"
        elif isinstance(
            value,
            (
                int,
                float,
            ),
        ):
            return f"{value}"
        elif isinstance(value, (datetime,)):
            return f"cast('{value.strftime('%Y-%m-%d %H:%M:%S')}' as timestamp)"
        elif isinstance(value, (date,)):
            return f"cast ('{value.strftime('%Y-%m-%d')}' as date)"
        else:
            raise SqlProcessorAssertionError(f"value of type {type(value)} not supported yet!")

    def for_bigquery_type(self, col_name: str, col_type: Union[str, TypeEngine]) -> str:
        if self.column_sql_type_converter:
            converted_col_type = self.column_sql_type_converter("bigquery", col_name, col_type)  # type: ignore
            if converted_col_type is not None:
                return converted_col_type

        if str(col_type.__class__) == "<class 'str'>":
            return col_type  # type: ignore

        import sqlalchemy

        if isinstance(col_type, (sqlalchemy.FLOAT, sqlalchemy.Float)):
            return "FLOAT64"
        elif isinstance(col_type, sqlalchemy.VARCHAR):
            return "STRING"
        else:
            return col_type.__class__.__name__


class SqlDialect:
    def __init__(self, sql_expr: SqlExpr):
        self.sql_expr = sql_expr

    def create_partition_automatically(self) -> bool:
        raise NotImplementedError()

    def support_static_partition(self) -> bool:
        raise NotImplementedError()

    def support_native_partition(self) -> bool:
        raise NotImplementedError()

    def support_move_individual_partition(self) -> bool:
        raise NotImplementedError()

    def create_db_sql(self, db: str) -> str:
        raise NotImplementedError()

    def use_db_sql(self, db: str) -> str:
        raise NotImplementedError()

    def drop_db_sql(self, db: str) -> Union[str, List[str]]:
        raise NotImplementedError()

    def rename_table_sql(self, from_table: str, to_table: str) -> str:
        raise NotImplementedError()

    def rename_table_db_sql(self, table_name: str, schema: str):
        raise NotImplementedError()

    def get_tables_sql(self, db) -> str:
        raise NotImplementedError()

    def get_dbs_sql(self) -> str:
        raise NotImplementedError()

    def create_table_sql(self, table_name: str, select_sql: str) -> str:
        raise NotImplementedError()

    def rename_view_sql(self, from_table: str, to_table: str) -> str:
        raise NotImplementedError()

    def drop_view_sql(self, table: str) -> str:
        raise NotImplementedError()

    def create_view_sql(self, table_name: str, select_sql: str) -> str:
        raise NotImplementedError()

    def delete_partition_sql(self, table_name, partitions: List[Partition]) -> Union[str, List[str]]:
        raise NotImplementedError()

    def native_partitions_sql(self, table_name: str) -> Tuple[str, Callable[[ResultProxy], List[str]]]:
        raise NotImplementedError()

    def create_table_with_partitions_sql(self, table_name: str, cols: List[Dict], partitions: List[Partition]):
        raise NotImplementedError()

    def create_partitions_with_data_sqls(
        self, source_table_name: str, target_table_name: str, col_names: List[str], partitions: List[List[Partition]]
    ):
        raise NotImplementedError()

    def create_partition_sql(
        self, target_table_name: str, partitions: List[Partition], if_not_exists: bool = False
    ) -> str:
        raise NotImplementedError()

    def convert_pt_col_expr(self, all_cols: List[str], partition_cols: List[str]) -> List[str]:
        if len(partition_cols) == 0:
            return all_cols
        else:
            if len(partition_cols) > 1:
                raise SqlProcessorAssertionError(
                    f"Only single-column partitioning is supported! found: {partition_cols}"
                )
            # the format of pt_col may be :{pt_col}, which is transferred by method create_table_with_data
            return [
                col
                if col not in [partition_cols[0], f":{partition_cols[0]}"]
                else self.sql_expr.convert_partition_col(col)
                for col in all_cols
            ]

    def insert_data_sql(
        self, table_name: str, col_names_expr: str, select_sql: str, partitions: List[Partition]
    ) -> Union[str, List[str]]:
        raise NotImplementedError()

    def move_data_sql(self, target_table_name: str, temp_table_name: str, partitions: List[Partition]) -> List[str]:
        raise NotImplementedError()

    def drop_table_sql(self, table: str) -> str:
        raise NotImplementedError()

    def create_pt_meta_table_sql(self, db: str) -> str:
        raise NotImplementedError()

    def insert_pt_metadata_sql(self, table_name: str, partitions: List[Partition]) -> str:
        raise NotImplementedError()

    def delete_pt_metadata_sql(self, table_name: str, partitions: List[Partition]) -> str:
        raise NotImplementedError()

    def create_table_like_sql(self, target_table_name: str, source_table_name: str, partitions: List[Partition]) -> str:
        raise NotImplementedError()
