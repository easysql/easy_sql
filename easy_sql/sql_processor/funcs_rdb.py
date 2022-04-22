from typing import List

from .backend.rdb import RdbBackend
from .funcs_common import ColumnFuncs, TableFuncs, PartitionFuncs as PartitionFuncsBase, AlertFunc

__all__ = [
    'PartitionFuncs', 'ColumnFuncs', 'AlertFunc', 'TableFuncs'
]


class PartitionFuncs(PartitionFuncsBase):

    def __check_backend(self):
        if not isinstance(self.backend, RdbBackend):
            msg = f'Backend of type {type(self.backend)}-{self.backend.backend_type if isinstance(self.backend, RdbBackend) else ""} ' \
                  f'is not supported yet'
            raise Exception(msg)

    def _get_bigquery_partition_values(self, table_name):
        db, table = self.__parse_table_name(table_name)
        sql = f"select partition_value from {db}.__table_partitions__ where table_name = '{table}' order by partition_value"
        partition_values = [str(v[0]) for v in self.backend.exec_sql(sql).collect()]
        return partition_values

    def _get_clickhouse_partition_values(self, table_name):
        sql = f"SELECT partition FROM {self.backend.partitions_table_name} where table = {table_name};"
        partition_values = [str(v[0]) for v in self.backend.exec_sql(sql).collect()]
        partition_values.sort()
        return partition_values

    def _get_postgresql_partition_values(self, table_name):
        db, table = self.__parse_table_name(table_name)
        sql = f'''
        SELECT
            concat(nmsp_child.nspname, '.', child.relname) as partition_tables,
            pg_catalog.pg_get_expr(child.relpartbound, child.oid) as partition_expr
        FROM pg_inherits
            JOIN pg_class parent        ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child         ON pg_inherits.inhrelid   = child.oid
            JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
            JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
            JOIN pg_partitioned_table part  ON part.partrelid = parent.oid
        WHERE nmsp_parent.nspname='{db}' and parent.relname='{table}'
        '''
        partition_values = [str(v[1]) for v in self.backend.exec_sql(sql).collect()]
        for p in partition_values:
            if not p.upper().startswith('FOR VALUES FROM (') or not ') TO (' in p.upper():
                raise Exception('unable to parse partition: ' + p)
        partition_values = [v[len('FOR VALUES FROM ('):v.upper().index(') TO (')] for v in partition_values]
        partition_values = [v.strip("'") if v.startswith("'") else int(v) for v in partition_values]
        partition_values.sort()
        return partition_values

    def _get_partition_values(self, table_name):
        if not isinstance(self.backend, RdbBackend):
            raise Exception(f"Backend of type {type(self.backend)} not supported, only support for RdbBackend")
        if self.backend.is_pg:
            return self._get_postgresql_partition_values(table_name)
        elif self.backend.is_ch:
            return self._get_clickhouse_partition_values(table_name)
        elif self.backend.is_bq:
            return self._get_bigquery_partition_values(table_name)
        else:
            msg = f'Backend of type {type(self.backend)}-{self.backend.backend_type if isinstance(self.backend, RdbBackend) else ""} ' \
                  f'is not supported yet'
            raise Exception(msg)

    def get_partition_cols(self, table_name: str) -> List[str]:
        self.__check_backend()
        db, table = self.__parse_table_name(table_name)
        sql = f'''
        SELECT pg_catalog.pg_get_partkeydef(pt_table.oid) as partition_key
        FROM pg_class pt_table
            JOIN pg_namespace nmsp_pt_table   ON nmsp_pt_table.oid  = pt_table.relnamespace
        WHERE nmsp_pt_table.nspname='{db}' and pt_table.relname='{table}'
        '''
        partition_values = [v[0] for v in self.backend.exec_sql(sql).collect()]
        if not partition_values:
            raise Exception('no partition values found, table may not exist')
        partition_value = partition_values[0]
        if partition_value is None:
            return []
        partition_value: str = partition_value
        if not partition_value.upper().startswith('RANGE (') or not partition_value.endswith(')'):
            raise Exception('unable to parse partition: ' + partition_value)
        return partition_value[len('RANGE ('): -1].split(',')

    def __parse_table_name(self, table_name):
        backend: RdbBackend = self.backend
        full_table_name = table_name if '.' in table_name else f'{backend.temp_schema}.{table_name}'
        db, table = full_table_name[:full_table_name.index('.')], full_table_name[full_table_name.index('.') + 1:]
        return db, table
