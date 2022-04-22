import re
import time
import unittest
from datetime import datetime

from easy_sql import base_test
from easy_sql.base_test import dt, date
from easy_sql.sql_processor.backend import TableMeta, SaveMode, Partition
from easy_sql.sql_processor.backend.rdb import RdbBackend, RdbRow, _exec_sql, TimeLog, SqlExpr

partition_col_converter = lambda col: \
    f"PARSE_DATE('%Y-%m', {col}) as {col}" if col in ['data_month', ":data_month"] else f"CAST({col} as DATE)"
partition_value_converter = lambda col, value: \
    datetime.strptime(value, '%Y-%m').date() if col == 'data_month' else datetime.strptime(value, '%Y-%m-%d').date()
column_sql_type_converter = lambda backend_type, col_name, col_type: \
    'DATE' if col_name in ['di', 'dt', 'data_date', 'data_month'] else None
partition_expr = lambda backend_type, partition_col: \
    f'DATE_TRUNC({partition_col}, MONTH)' if backend_type == 'bigqiery' and partition_col == 'data_month' else partition_col
sql_expr = SqlExpr(column_sql_type_converter=column_sql_type_converter,
                   partition_col_converter=partition_col_converter, partition_value_converter=partition_value_converter,
                   partition_expr=partition_expr)


class RdbTest(unittest.TestCase):

    def test_log_time(self):
        with TimeLog('start', 'end({time_took:.3f})'):
            time.sleep(0.1)

    def test_clean_pg_temp_schema(self):
        if not base_test.should_run_integration_test('pg'):
            return

        pg = RdbBackend('postgresql://postgres:123456@testpg:15432/postgres')
        from sqlalchemy import inspect
        from sqlalchemy.engine.reflection import Inspector
        insp: Inspector = inspect(pg.engine)
        temp_schemas = insp.get_schema_names()
        for schema in temp_schemas:
            if schema.startswith('sp_temp_'):
                _exec_sql(pg.conn, f'drop schema if exists {schema} cascade')

    def test_clean_ch_temp_schema(self):
        if not base_test.should_run_integration_test('ch'):
            return
        backend = RdbBackend('clickhouse+native://default@testch:30123')
        from sqlalchemy import inspect
        from sqlalchemy.engine.reflection import Inspector
        insp: Inspector = inspect(backend.engine)
        temp_schemas = insp.get_schema_names()
        for schema in temp_schemas:
            if schema.startswith('sp_temp_'):
                _exec_sql(backend.conn, f'drop database if exists {schema}')

    def test_clean_bq_temp_schema(self):
        if not base_test.should_run_integration_test('bq'):
            return
        import os
        backend = RdbBackend('bigquery://',
                             credentials=f"{os.environ.get('HOME', '/tmp')}/.bigquery/credential-test.json",
                             sql_expr=bq_sql_expr)
        from sqlalchemy import inspect
        from sqlalchemy.engine.reflection import Inspector
        insp: Inspector = inspect(backend.engine)
        temp_schemas = insp.get_schema_names()
        for schema in temp_schemas:
            if schema.startswith('sp_temp_'):
                _exec_sql(backend.conn, f'drop schema if exists {schema} cascade')

    def test_pg_table(self):
        if not base_test.should_run_integration_test('pg'):
            return
        backend = RdbBackend('postgresql://postgres:123456@testpg:15432/postgres')
        _exec_sql(backend.conn, 'drop schema if exists t cascade')
        _exec_sql(backend.conn, 'create schema t')
        _exec_sql(backend.conn, 'create table t.test(id int, val varchar(100))')
        _exec_sql(backend.conn, "insert into t.test values(1, '1'), (2, '2'), (3, '3')")
        self.run_test_table(backend)

    def test_ch_table(self):
        if not base_test.should_run_integration_test('ch'):
            return
        backend = RdbBackend('clickhouse+native://default@testch:30123')
        _exec_sql(backend.conn, 'drop database if exists t')
        _exec_sql(backend.conn, 'create database t')
        _exec_sql(backend.conn, 'create table t.test(id int, val String) engine=MergeTree order by tuple()')
        _exec_sql(backend.conn, "insert into t.test values(1, '1'), (2, '2'), (3, '3')")
        self.run_test_table(backend)

    def test_bq_table(self):
        if not base_test.should_run_integration_test('bq'):
            return
        import os
        backend = RdbBackend('bigquery://',
                             credentials=f"{os.environ.get('HOME', '/tmp')}/.bigquery/credential-test.json",
                             sql_expr=bq_sql_expr)
        _exec_sql(backend.conn, 'drop schema if exists t cascade')
        _exec_sql(backend.conn, 'create schema if not exists t')
        _exec_sql(backend.conn, 'drop table if exists t.test')
        _exec_sql(backend.conn, 'create table if not exists t.test(id int, val string)')
        _exec_sql(backend.conn, "insert into t.test values(1, '1'), (2, '2'), (3, '3')")

        from datetime import timedelta, timezone
        tz = timezone(timedelta(hours=0))
        self.run_test_table(backend, timezone=tz)

    def test_pg_backend(self):
        if not base_test.should_run_integration_test('pg'):
            return
        backend = RdbBackend('postgresql://postgres:123456@testpg:15432/postgres')
        _exec_sql(backend.conn, 'drop schema if exists t cascade')
        _exec_sql(backend.conn, 'create schema t')
        _exec_sql(backend.conn, 'create table t.test(id int, val varchar(100))')
        _exec_sql(backend.conn, "insert into t.test values(1, '1'), (2, '2'), (3, '3')")

        self.run_test_backend(backend)

    def test_ch_backend(self):
        if not base_test.should_run_integration_test('ch'):
            return
        backend = RdbBackend('clickhouse+native://default@testch:30123')
        _exec_sql(backend.conn, 'drop database if exists t')
        _exec_sql(backend.conn, 'create database t')
        _exec_sql(backend.conn, 'create table t.test(id int, val String) engine=MergeTree order by tuple()')
        _exec_sql(backend.conn, "insert into t.test values(1, '1'), (2, '2'), (3, '3')")
        self.run_test_backend(backend)

    def test_bq_backend(self):
        if not base_test.should_run_integration_test('bq'):
            return
        import os
        backend = RdbBackend('bigquery://',
                             credentials=f"{os.environ.get('HOME', '/tmp')}/.bigquery/credential-test.json",
                             sql_expr=bq_sql_expr)
        _exec_sql(backend.conn, 'drop schema if exists t cascade')
        _exec_sql(backend.conn, 'create schema if not exists t')
        _exec_sql(backend.conn, 'create table if not exists t.test(id int, val string)')
        _exec_sql(backend.conn, "insert into t.test values(1, '1'), (2, '2'), (3, '3')")

        from datetime import timedelta, timezone
        tz = timezone(timedelta(hours=0))
        self.run_test_backend(backend)

    def run_test_table(self, backend: RdbBackend, timezone=None):
        table = backend.exec_sql('select * from t.test')
        self.assertFalse(table.is_empty())

        table = table.limit(0)
        self.assertTrue(table.is_empty())

        table = backend.exec_sql('select * from t.test order by id').with_column('a', backend.sql_expr.for_value(1))
        self.assertFalse(table.is_empty())
        self.assertTrue(table.count(), 3)
        self.assertListEqual(table.collect(), [
            RdbRow(['id', 'val', 'a'], (1, '1', 1)),
            RdbRow(['id', 'val', 'a'], (2, '2', 1)),
            RdbRow(['id', 'val', 'a'], (3, '3', 1))
        ])
        self.assertEqual(table.first(), RdbRow(['id', 'val', 'a'], (1, '1', 1)))
        self.assertListEqual(table.field_names(), ['id', 'val', 'a'])
        self.assertEqual(table.limit(2).count(), 2)

        table = backend.exec_sql('select * from t.test order by id').with_column('a', backend.sql_expr.for_value('1'))
        self.assertEqual(table.first(), RdbRow(['id', 'val', 'a'], (1, '1', '1')))

        table = backend.exec_sql('select * from t.test order by id').with_column('a', backend.sql_expr.for_value(1.1))
        self.assertEqual(table.first(), RdbRow(['id', 'val', 'a'], (1, '1', 1.1)))

        table = backend.exec_sql('select * from t.test order by id').with_column('a', backend.sql_expr.for_value(dt('2020-01-01 11:11:11')))
        self.assertEqual(table.first(),
                         RdbRow(['id', 'val', 'a'], (1, '1', base_test.dt_zone('2020-01-01 11:11:11', timezone=timezone))))

        table.show(1)

    def run_test_backend(self, backend: RdbBackend):
        backend.create_empty_table()  # should not raise exception

        backend.create_temp_table(backend.exec_sql('select * from t.test order by id limit 2'), 'test_limit')
        self.assertListEqual(backend.exec_sql(f'select * from {backend.temp_schema}.test_limit order by id').collect(),
                             [
                                 RdbRow(['id', 'val'], (1, '1')), RdbRow(['id', 'val'], (2, '2'))
                             ])

        self.assertTrue(backend.table_exists(TableMeta('t.test')))
        self.assertFalse(backend.table_exists(TableMeta('t.test_xx')))
        self.assertTrue(backend.table_exists(TableMeta('test_limit')))

        # save without transformation or partitions, should create
        backend.create_cache_table(backend.exec_sql('select * from t.test'), 'test')
        backend.save_table(TableMeta('test'), TableMeta('t.xx0'), SaveMode.overwrite, True)
        self.assertListEqual(backend.exec_sql(f'select * from t.xx0 order by id').collect(), [
            RdbRow(['id', 'val'], (1, '1')), RdbRow(['id', 'val'], (2, '2')), RdbRow(['id', 'val'], (3, '3'))
        ])

        self.assertRaisesRegex(Exception, re.compile(r'.* cannot save table.*'),
                               lambda: backend.save_table(TableMeta('test_limit'), TableMeta('t.xx'), SaveMode.overwrite,
                                                          False))

        # first save without partitions, should create
        backend.save_table(TableMeta('test_limit'), TableMeta('t.xx'), SaveMode.overwrite, True)
        self.assertListEqual(backend.exec_sql(f'select * from t.xx order by id').collect(), [
            RdbRow(['id', 'val'], (1, '1')), RdbRow(['id', 'val'], (2, '2'))
        ])

        # second save without partitions, should overwrite
        backend.exec_native_sql(backend.db_config.drop_view_sql('test_limit'))
        backend.create_cache_table(backend.exec_sql('select * from t.test order by id limit 2'), 'test_limit')
        backend.save_table(TableMeta('test_limit'), TableMeta('t.xx'), SaveMode.overwrite, True)
        self.assertListEqual(backend.exec_sql(f'select * from t.xx order by id').collect(), [
            RdbRow(['id', 'val'], (1, '1')), RdbRow(['id', 'val'], (2, '2'))
        ])

        if backend.is_bq:
            mock_dt_1, mock_dt_2 = date('2021-01-01'), date('2021-01-02')
        elif backend.is_pg:
            mock_dt_1, mock_dt_2 = '2021-01-01', '2021-01-02'
        else:
            mock_dt_1, mock_dt_2 = dt('2021-01-01 00:00:00'), dt('2021-01-02 00:00:00')

        backend.exec_native_sql(backend.db_config.drop_view_sql('test_limit'))
        backend.create_cache_table(backend.exec_sql('select id from t.test limit 2'), 'test_limit')
        self.assertRaisesRegex(Exception, re.compile(r'source_cols does not contain target_cols.*'),
                               lambda: backend.save_table(TableMeta('test_limit'),
                                                          TableMeta('t.xx', partitions=[
                                                              Partition('a', mock_dt_1)]),
                                                          SaveMode.overwrite, True))

        _exec_sql(backend.conn, f'drop table if exists t.xx')

        # first save with partitions, should create
        backend.exec_native_sql(backend.db_config.drop_view_sql('test_limit'))
        backend.create_cache_table(backend.exec_sql('select * from t.test order by id limit 2'), 'test_limit')
        backend.save_table(TableMeta('test_limit'),
                           TableMeta('t.xx', partitions=[Partition('a', mock_dt_1)]), SaveMode.overwrite,
                           True)
        self.assertListEqual(backend.exec_sql(f'select * from t.xx order by id').collect(), [
            RdbRow(['id', 'val', 'a'], (1, '1', mock_dt_1)),
            RdbRow(['id', 'val', 'a'], (2, '2', mock_dt_1))
        ])

        # second save with partitions, should overwrite
        backend.exec_native_sql(backend.db_config.drop_view_sql('test_limit'))
        backend.create_cache_table(backend.exec_sql('select * from t.test order by id limit 2'), 'test_limit')
        backend.save_table(TableMeta('test_limit'),
                           TableMeta('t.xx', partitions=[Partition('a', mock_dt_2)]), SaveMode.overwrite,
                           True)
        self.assertListEqual(backend.exec_sql(f'select * from t.xx order by a, id').collect(), [
            RdbRow(['id', 'val', 'a'], (1, '1', mock_dt_1)),
            RdbRow(['id', 'val', 'a'], (2, '2', mock_dt_1)),
            RdbRow(['id', 'val', 'a'], (1, '1', mock_dt_2)),
            RdbRow(['id', 'val', 'a'], (2, '2', mock_dt_2))
        ])

        # third save with partitions, should overwrite
        backend.exec_native_sql(backend.db_config.drop_view_sql('test_limit'))
        backend.create_cache_table(backend.exec_sql('select * from t.test order by id limit 2'), 'test_limit')
        backend.save_table(TableMeta('test_limit'),
                           TableMeta('t.xx', partitions=[Partition('a', mock_dt_2)]), SaveMode.overwrite,
                           False)
        self.assertListEqual(backend.exec_sql(f'select * from t.xx order by a, id').collect(), [
            RdbRow(['id', 'val', 'a'], (1, '1', mock_dt_1)),
            RdbRow(['id', 'val', 'a'], (2, '2', mock_dt_1)),
            RdbRow(['id', 'val', 'a'], (1, '1', mock_dt_2)),
            RdbRow(['id', 'val', 'a'], (2, '2', mock_dt_2))
        ])

        # fourth save with partitions, should append
        backend.exec_native_sql(backend.db_config.drop_view_sql('test_limit'))
        backend.create_cache_table(backend.exec_sql('select * from t.test order by id limit 2'), 'test_limit')
        backend.save_table(TableMeta('test_limit'),
                           TableMeta('t.xx', partitions=[Partition('a', mock_dt_2)]), SaveMode.append,
                           True)
        self.assertListEqual(backend.exec_sql(f'select * from t.xx order by a, id').collect(), [
            RdbRow(['id', 'val', 'a'], (1, '1', mock_dt_1)),
            RdbRow(['id', 'val', 'a'], (2, '2', mock_dt_1)),
            RdbRow(['id', 'val', 'a'], (1, '1', mock_dt_2)),
            RdbRow(['id', 'val', 'a'], (1, '1', mock_dt_2)),
            RdbRow(['id', 'val', 'a'], (2, '2', mock_dt_2)),
            RdbRow(['id', 'val', 'a'], (2, '2', mock_dt_2)),
        ])


if __name__ == '__main__':
    unittest.main()
