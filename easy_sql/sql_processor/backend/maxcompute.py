from __future__ import annotations

import time
from enum import Enum
from random import random
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

from ...logger import logger
from .base import Backend, Row, SaveMode, Table

if TYPE_CHECKING:
    from .base import TableMeta

# TODO: SqlExpr should be a common class
from .rdb import SqlExpr


def _exec_sql(conn, sql: str):
    logger.info(f"will exec sql: {sql}")
    conn.execute_sql(sql)


class PartitionMode(Enum):
    ALL_DYNAMIC = (0,)
    ALL_STATIC = (1,)
    HYBRID = 2


class MaxComputeRow(Row):
    def __init__(self, schema, values: Optional[Tuple]):
        from odps.types import Record

        self.row = Record(schema=schema, values=values)
        self.columns = [c.name for c in schema.columns]
        self.types = [c.type.name for c in schema.columns]
        self.values = self.row.values

    @staticmethod
    def from_schema_meta(
        cols: List[str],
        types: List[str],
        values: Optional[Tuple],
        pt_cols: List[str] = None,
        pt_types: List[str] = None,
    ):
        from odps.types import OdpsSchema

        schema = OdpsSchema.from_lists(cols, types, partition_names=pt_cols, partition_types=pt_types)
        return MaxComputeRow(schema=schema, values=values)

    def as_dict(self):
        return dict(self.row)

    def as_tuple(self) -> Tuple:
        return self.row

    def __eq__(self, other):
        if not isinstance(other, MaxComputeRow):
            return False
        return self.columns == other.columns and self.values == other.values

    def __str__(self):
        return str(self.row)[12:]

    def __getitem__(self, i):
        return self.row[i]

    def __repr__(self):
        return self.row.__repr__()


class MaxComputeTable(Table):
    def __init__(self, backend: MaxComputeBackend, sql: str, table_name: str = None):
        self.sql = sql
        self.backend = backend
        # TODO: consider and the job id, for batch delete views
        # TODO: eg. backend.job_id, as to the part of temp table name
        # TODO: table_name need with suffix to avoid parallel run
        self.table_name = table_name or f"t_{int(time.mktime(time.gmtime()))}_{int(random() * 10000):04d}"
        self.backend.conn.execute_sql(f"create or replace view {self.table_name} as {sql}")
        # The MaxCompute view can not be read when using table frame, must transform to dataframe
        self.df = self.backend.get_table(self.table_name).to_df()
        self.backend.append_temp_view(self.table_name)
        from odps.types import OdpsSchema

        self.schema = OdpsSchema.from_lists(self.df.data.schema.names, self.df.data.schema.types)

    @staticmethod
    def from_table_meta(backend, table_meta: TableMeta):
        table = MaxComputeTable(backend, f"select * from {table_meta.table_name}")
        if table_meta.has_partitions():
            for pt in table_meta.partitions:
                if pt.field not in table.field_names():
                    table = table.with_column(pt.field, pt.value)
                else:
                    if pt.value is not None:
                        raise Exception(
                            f"partition column already exists in table {table_meta.table_name}, "
                            f"but right now we provided a new value {pt.value} for partition column {pt.field}"
                        )
        return table

    def is_empty(self) -> bool:
        return self.count() == 0

    def field_names(self) -> List[str]:
        return self.schema.names

    def field_types(self) -> List[str]:
        return self.schema.types

    def first(self) -> MaxComputeRow:
        import numpy

        row = [x.values for x in self.df.head(1)]
        value = row[0] if row else None
        value = [bool(v) if isinstance(v, numpy.bool_) else v for v in value]
        return MaxComputeRow(schema=self.schema, values=tuple(value))

    def limit(self, count: int) -> MaxComputeTable:
        return MaxComputeTable(self.backend, f"select * from {self.table_name} limit {count}")

    def with_column(self, name: str, value: any) -> MaxComputeTable:
        value_expr = self.backend.sql_expr.for_value(value).replace("''", "'")
        return MaxComputeTable(self.backend, f"select *, {value_expr} as {name} from {self.table_name}")

    def collect(self, count: int = 1000) -> List[Row]:
        return [MaxComputeRow(schema=self.schema, values=tuple(r.values)) for r in self.df.head(count)]

    def show(self, count: int = 20):
        print("\t".join(self.field_names()))
        for record in self.df.head(20):
            print("\t".join([f"{x}" for x in record.values]))

    def count(self) -> int:
        return self.df.count().execute()

    def __repr__(self):
        return self.table_name

    # TODO: common code with Table.__partition_expr__, consider add a util class
    def __partition_expr__(self, target_table_meta: TableMeta, partition_mode: PartitionMode) -> str:
        if not target_table_meta.partitions:
            return ""

        dynamic_partitions = [p for p in target_table_meta.partitions if not p.value]
        static_partitions = [p for p in target_table_meta.partitions if p.value]

        if partition_mode == PartitionMode.ALL_DYNAMIC:
            fields = [f"{p.field}" for p in target_table_meta.partitions]
        elif partition_mode == PartitionMode.HYBRID:
            fields = [f"{p.name}='{p.value}'" for p in static_partitions] + [f"{p.field}" for p in dynamic_partitions]
        else:
            if dynamic_partitions:
                raise Exception(
                    "In ALL_STATIC partition mode, no dynamic partition is supported. "
                    f"Now there are the following dynamic partitions: {dynamic_partitions}"
                )
            fields = [f"{p.field}='{p.value}'" for p in target_table_meta.partitions]

        return f"partition ({','.join(fields)})"

    def save_to_table(self, target_table_meta: TableMeta, save_mode: SaveMode = SaveMode.overwrite) -> None:
        dynamic_partitions = [p for p in target_table_meta.partitions if not p.value]
        static_partitions = [p for p in target_table_meta.partitions if p.value]
        target_table = self.backend.get_table(target_table_meta.table_name)

        temp_res = self
        if dynamic_partitions:
            for pt in static_partitions:
                temp_res = self.with_column(pt.field, pt.value)
            target_cols = [c.name for c in target_table.schema.columns]
            partition_expr = self.__partition_expr__(target_table_meta, PartitionMode.ALL_DYNAMIC)

        else:
            target_cols = target_table.schema.names
            partition_expr = self.__partition_expr__(target_table_meta, PartitionMode.ALL_STATIC)

        temp_res = self.backend.exec_sql(f"select {', '.join(target_cols)} from {temp_res.table_name}")

        self.backend.exec_native_sql(
            f"""
            insert {'into' if save_mode == SaveMode.append else save_mode.name}
            table {target_table_meta.table_name}
            {partition_expr}
            select * from {temp_res.table_name}
        """
        )


class MaxComputeBackend(Backend):
    def __init__(self, sql_expr: Optional[SqlExpr], **kwargs):
        from odps import ODPS

        self.conn = ODPS(**kwargs)
        self.temp_views = []
        self.sql_expr = sql_expr or SqlExpr()

        from odps import options

        # Support timestamp
        options.sql.use_odps2_extension = True
        # TODO: Support full scan for partitioned table. Should forbid?
        options.sql.settings = {"odps.sql.allow.fullscan": True}

    def get_table(self, table_name):
        return self.conn.get_table(table_name)

    def init_udfs(self, *args, **kwargs):
        pass

    def register_udfs(self, funcs: Dict[str, Callable]):
        pass

    def create_empty_table(self):
        # TODO: empty table?
        return MaxComputeTable(self, "select 1 as a")

    def exec_native_sql(self, sql: str) -> Any:
        logger.info(f"will exec sql: {sql}")
        return self.conn.execute_sql(sql)

    def exec_sql(self, sql: str) -> MaxComputeTable:
        # TODO: extract all temp table names and replace as the mapped temp view name
        return MaxComputeTable(self, sql)

    def temp_tables(self) -> List[str]:
        return self.temp_views

    def append_temp_view(self, view_name):
        self.temp_views.append(view_name)

    def clear_cache(self):
        pass

    def clean(self):
        self.clear_temp_tables()

    def _clear_temp_views(self, exclude: List[str] = None):
        # TODO: consider view dependencies
        for temp_view in self.temp_views:
            if exclude and temp_view in exclude:
                continue
            self.conn.execute_sql(f"drop view if exists {temp_view}")

    def clear_temp_tables(self, exclude: List[str] = None):
        self._clear_temp_views(exclude)

    def create_temp_table(self, table: MaxComputeTable, name: str):
        logger.info(f"create_temp_table with: table={table}, name={name}")
        self.conn.execute_sql(f"create or replace view {name} as select * from {table.table_name}")
        self.append_temp_view(name)

    def create_cache_table(self, table: MaxComputeTable, name: str):
        logger.info(f"create_cache_table with: table={table}, name={name}")
        self.conn.execute_sql(f"create or replace view {name} as select * from {table.table_name}")
        self.append_temp_view(name)

    def broadcast_table(self, table: MaxComputeTable, name: str):
        logger.info(f"broadcast_table with: table={table}, name={name}")
        self.conn.execute_sql(f"create or replace view {name} as select * from {table.table_name}")
        self.append_temp_view(name)

    def table_exists(self, table: TableMeta):
        return self.conn.exist_table(table.table_name)

    def refresh_table_partitions(self, table: TableMeta):
        pass

    def save_table(
        self,
        source_table_meta: TableMeta,
        target_table_meta: TableMeta,
        save_mode: SaveMode,
        create_target_table: bool,
    ):
        logger.info(
            f"save table with: source_table={source_table_meta.table_name}, "
            f"target_table={target_table_meta.table_name}, "
            f"save_mode={save_mode}, create_target_table={create_target_table}"
        )
        if not self.table_exists(target_table_meta) and not create_target_table:
            raise Exception(
                f"target table {target_table_meta.table_name} does not exist, "
                "and create_target_table is False, "
                f"cannot save table {source_table_meta.table_name} to {target_table_meta.table_name}"
            )

        source_table = MaxComputeTable.from_table_meta(self, source_table_meta)

        if not self.table_exists(target_table_meta) and create_target_table:
            pt_names = [p.field for p in target_table_meta.partitions] or None
            pt_types = ["string"] * (len(target_table_meta.partitions) + 1) or None

            from odps.types import OdpsSchema

            self.conn.create_table(
                target_table_meta.table_name,
                schema=OdpsSchema.from_lists(
                    names=source_table.schema.names,
                    types=source_table.schema.types,
                    partition_names=pt_names,
                    partition_types=pt_types,
                ),
            )

        source_table.save_to_table(target_table_meta, save_mode)
