import unittest

from easy_sql.base_test import dt, TEST_PG_URL, TEST_CH_URL


class SqlProcessorTest(unittest.TestCase):

    def test_should_run_sql_for_pg_backend(self):
        from easy_sql.sql_processor.backend.rdb import RdbBackend, _exec_sql
        backend = RdbBackend(TEST_PG_URL)
        _exec_sql(backend.conn, 'drop schema if exists t cascade')
        _exec_sql(backend.conn, 'create schema t')
        self.run_sql_for_pg_backend(backend)

    def test_should_run_sql_for_ch_backend(self):
        from easy_sql.sql_processor.backend.rdb import RdbBackend, _exec_sql
        backend = RdbBackend(TEST_CH_URL)
        _exec_sql(backend.conn, 'drop database if exists t')
        _exec_sql(backend.conn, 'create database t')
        self.run_sql_for_pg_backend(backend)

    def test_should_run_sql_for_ch_backend_for_dynamic_partitions(self):
        from easy_sql.sql_processor.backend.rdb import RdbBackend, _exec_sql
        backend = RdbBackend(TEST_CH_URL)
        _exec_sql(backend.conn, 'drop database if exists t')
        _exec_sql(backend.conn, 'create database t')
        from easy_sql.sql_processor import SqlProcessor
        sql = '''
-- backend: clickhouse
-- target=variables
select '2021-01-01'       as __partition__data_date
-- target=temp.result
select 1 as a
-- target=output.t.result
select *, 2 as b from result
                '''
        processor = SqlProcessor(backend, sql, variables={'__create_output_table__': True})
        processor.func_runner.register_funcs({'t': lambda a, b: int(a) + int(b)})
        processor.run(dry_run=False)
        sql = '''
-- backend: clickhouse
-- target=temp.result1
select 1 as a
-- target=output.t.result
select *, 3 as b, '2021-01-01' as data_date from result1
-- target=output.t.result
select *, 3 as b, '2021-01-02' as data_date from result1
                    '''
        processor = SqlProcessor(backend, sql)
        processor.func_runner.register_funcs({'t': lambda a, b: int(a) + int(b)})
        processor.run(dry_run=False)
        self.assertEqual(backend.exec_sql('select * from t.result order by data_date').collect(), [
            (1, 3, '2021-01-01'), (1, 3, '2021-01-02')
        ])

    def run_sql_for_pg_backend(self, backend):
        from easy_sql.sql_processor import SqlProcessor
        sql = '''
-- backend: postgres
-- target=variables
select
    1                    as __create_output_table__
    , 'append'           as __save_mode__
    , '2021-01-01'       as __partition__data_date
-- target=temp.result
select 1 as a
-- target=temp.result1
select *, 2 as b from result
-- target=output.t.result
select *, 2 as b from result
-- target=output.t.result
select * from result1
-- target=output.t.result1
select 1 as a, 2 as b, 't' as c, cast('2021-01-01' as timestamp) as d, ${t(1, 2)} as e
-- target=output.t.result1
select 1 as a, 2 as b, 't' as c, cast('2021-01-01' as timestamp) as d, ${t(1, 2)} as e
        '''
        processor = SqlProcessor(backend, sql, variables={'__create_output_table__': True})
        processor.func_runner.register_funcs({'t': lambda a, b: int(a) + int(b)})
        processor.run(dry_run=False)
        self.assertEqual(backend.exec_sql('select * from t.result').collect(), [
            (1, 2, '2021-01-01'), (1, 2, '2021-01-01')
        ])
        self.assertEqual(backend.exec_sql('select * from t.result1').collect(), [
            (1, 2, 't', dt('2021-01-01 00:00:00'), 3, '2021-01-01'), (1, 2, 't', dt('2021-01-01 00:00:00'), 3, '2021-01-01')
        ])
