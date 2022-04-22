import unittest

from .sql_processor.backend import Backend, SparkBackend
from .sql_processor_debugger import *
from . import base_test


class SqlProcessorDebuggerTest(unittest.TestCase):

    def test_process_sql_debugger_spark(self):
        from .base_test import LocalSpark
        spark = LocalSpark.get()
        df = spark.createDataFrame(
            [
                (1, 'a'),
                (2, 'b'),
                (3, 'c'),
                (4, 'd'),
                (5, 'e')
            ],
            ['id', 'type']
        )
        df.createOrReplaceTempView('target')
        self.run_test_process_sql_debugger(SparkBackend(spark))

    def test_process_sql_debugger_pg(self):
        if not base_test.should_run_integration_test('pg'):
            return
        from easy_sql.sql_processor.backend.postgres import PostgresBackend
        from easy_sql.sql_processor.backend.postgres import _exec_sql
        pg = PostgresBackend('postgresql://postgres:123456@testpg:15432/postgres')
        _exec_sql(pg.conn, 'drop schema if exists t cascade')
        _exec_sql(pg.conn, 'create schema t')
        _exec_sql(pg.conn, 'create table t.target(id int, type varchar(100))')
        _exec_sql(pg.conn, "insert into t.target values(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
        pg.create_temp_table(pg.exec_sql('select * from t.target'), 'target')
        self.run_test_process_sql_debugger(pg)

    def test_process_sql_debugger_ch(self):
        if not base_test.should_run_integration_test('ch'):
            return
        from easy_sql.sql_processor.backend.rdb import RdbBackend, _exec_sql
        backend = RdbBackend('clickhouse+native://default@testch:30123')
        _exec_sql(backend.conn, 'drop database if exists t')
        _exec_sql(backend.conn, 'create database t')
        _exec_sql(backend.conn, 'create table t.target(id int, type String) engine=MergeTree order by tuple()')
        _exec_sql(backend.conn, "insert into t.target values(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
        backend.create_temp_table(backend.exec_sql('select * from t.target'), 'target')
        self.run_test_process_sql_debugger(backend)

    def run_test_process_sql_debugger(self, backend: Backend):
        sql_content = '''
-- target=template.test_a
select
    *
from target
where
    type = 'a';
-- target=variables
select 1 as a
-- target=variables
select '${f2(1, 2)}' as b
-- target=log.b
select '${b}' as b
-- target=log.test_log
select 1 as t
-- target=check.test_check
select 1 as actual, 1 as expected
-- target=check.check(1, 2), if=f1(1, ${a})
-- target=template.test_b
-- for ch, we must use sub query, 
-- since `#{type} as type` in select expression conflicts with `target.type` in where expression
select 
    id as id,
    #{type} as type
from (
    select
        id as id
    from target
    where
        type = #{type}
) t;
-- target=template.test_c
select
    id as id,
    #{type2} as type
from (
    select 
        id as id
    from target
    where
        type = #{type1}
) t;

-- target=temp.test_result
select * from (
    @{test_a()}
) t -- comment
--
union all
select * from (
    @{test_b(type='b')}
) t1
union all
select * from (
    @{test_c(type1='c', type2='${c}')}
) t2
        '''

        py_funcs_content = '''

check = lambda a, b: a and b

f1 = lambda a, b: a and b

f2 = lambda a, b: a + b
        '''
        with open('/tmp/test_sql_processor_debugger.sql', 'w') as f:
            f.write(sql_content)
        with open('/tmp/test_sql_processor_debugger_funcs.py', 'w') as f:
            f.write(py_funcs_content)
        variables = {'c': 'c'}
        debugger = SqlProcessorDebugger('/tmp/test_sql_processor_debugger.sql', backend,
                                        variables, {}, '/tmp/test_sql_processor_debugger_funcs.py', [])

        debugger.print_steps()
        debugger.report()
        self.assertTrue(len(debugger.tempviews) in [1, 2])

        self.assertEqual(debugger.current_step_no, None)
        self.assertEqual(debugger.next_step_no, 1)
        self.assertEqual(debugger.last_step_no, None)
        self.assertEqual(debugger.left_step_count, len(debugger.steps))

        debugger.step_on()
        self.assertEqual(debugger.current_step_no, 1)
        self.assertEqual(debugger.next_step_no, 2)
        self.assertEqual(debugger.last_step_no, None)
        self.assertEqual(debugger.left_step_count, len(debugger.steps) - 1)

        debugger.step_to(3)
        self.assertEqual(debugger.current_step_no, 3)
        self.assertEqual(debugger.next_step_no, 4)
        self.assertEqual(debugger.last_step_no, 2)

        debugger.run()
        self.assertEqual(debugger.current_step_no, len(debugger.steps))
        self.assertEqual(debugger.next_step_no, None)
        self.assertEqual(debugger.last_step_no, len(debugger.steps) - 1)
        self.assertEqual(debugger.left_step_count, 0)

        self.assertListEqual(debugger.sql('select * from test_result order by id').collect(), [
            (1, 'a'),
            (2, 'b'),
            (3, 'c'),
        ])

        debugger.restart()
        self.assertEqual(debugger.current_step_no, None)
        self.assertEqual(debugger.next_step_no, 1)
        self.assertEqual(debugger.last_step_no, None)
        self.assertEqual(debugger.left_step_count, len(debugger.steps))
        self.assertTrue(len(debugger.tempviews) in [1, 2])

        debugger.run()
        self.assertEqual(debugger.current_step_no, len(debugger.steps))
        self.assertEqual(debugger.next_step_no, None)
        self.assertEqual(debugger.last_step_no, len(debugger.steps) - 1)
        self.assertEqual(debugger.left_step_count, 0)

        self.assertListEqual(debugger.sql('select * from test_result order by id').collect(), [
            (1, 'a'),
            (2, 'b'),
            (3, 'c'),
        ])
        debugger.report()
