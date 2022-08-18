import unittest

from .base_test import TEST_CH_URL, TEST_PG_URL
from .sql_processor.backend import Backend, SparkBackend
from .sql_processor_debugger import SqlProcessorDebugger


class SqlProcessorDebuggerTest(unittest.TestCase):
    def test_process_sql_debugger_spark(self):
        from .base_test import LocalSpark

        spark = LocalSpark.get()
        df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")], ["id", "type"])
        df.createOrReplaceTempView("target")
        self.run_test_process_sql_debugger(SparkBackend(spark))

    def test_process_sql_debugger_pg(self):
        from easy_sql.sql_processor.backend.postgres import PostgresBackend, _exec_sql

        pg = PostgresBackend(TEST_PG_URL)
        _exec_sql(pg.conn, "drop schema if exists t cascade")
        _exec_sql(pg.conn, "create schema t")
        _exec_sql(pg.conn, "create table t.target(id int, type varchar(100))")
        _exec_sql(pg.conn, "insert into t.target values(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
        pg.create_temp_table(pg.exec_sql("select * from t.target"), "target")
        self.run_test_process_sql_debugger(pg)

    def test_process_sql_debugger_ch(self):
        from easy_sql.sql_processor.backend.rdb import RdbBackend, _exec_sql

        backend = RdbBackend(TEST_CH_URL)
        _exec_sql(backend.conn, "drop database if exists t")
        _exec_sql(backend.conn, "create database t")
        _exec_sql(backend.conn, "create table t.target(id int, type String) engine=MergeTree order by tuple()")
        _exec_sql(backend.conn, "insert into t.target values(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
        backend.create_temp_table(backend.exec_sql("select * from t.target"), "target")
        self.run_test_process_sql_debugger(backend)

    def run_test_process_sql_debugger(self, backend: Backend):
        sql_content = """
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
        """

        py_funcs_content = """

check = lambda a, b: a and b

f1 = lambda a, b: a and b

f2 = lambda a, b: a + b
        """
        with open("/tmp/test_sql_processor_debugger.sql", "w") as f:
            f.write(sql_content)
        with open("/tmp/test_sql_processor_debugger_funcs.py", "w") as f:
            f.write(py_funcs_content)
        variables = {"c": "c"}
        debugger = SqlProcessorDebugger(
            "/tmp/test_sql_processor_debugger.sql",
            backend,
            variables,
            {},
            "/tmp/test_sql_processor_debugger_funcs.py",
            [],
        )

        debugger.print_steps()
        debugger.report()
        self.assertTrue(len(debugger.tempviews) in [1, 2])

        self.assertEqual(debugger.current_step_no, None)
        self.assertEqual(debugger.next_step_no, 1)
        self.assertEqual(debugger.last_step_no, None)
        self.assertEqual(debugger.left_step_count, len(debugger.steps))
        self.assertFalse(debugger.is_started)
        self.assertFalse(debugger.is_finished)
        self.assertIsNone(debugger.current_step)
        self.assertIsNone(debugger.last_step)

        debugger.step_on()
        self.assertEqual(debugger.current_step_no, 1)
        self.assertEqual(debugger.next_step_no, 2)
        self.assertEqual(debugger.last_step_no, None)
        self.assertEqual(debugger.left_step_count, len(debugger.steps) - 1)
        self.assertTrue(debugger.is_started)
        self.assertFalse(debugger.is_finished)
        self.assertEqual(debugger.current_step, debugger.steps[0])
        debugger.step_to(1)  # will print info, but do nothing
        debugger.run_to(1)  # will print info, but do nothing

        debugger.step_to(3)
        self.assertEqual(debugger.current_step_no, 3)
        self.assertEqual(debugger.next_step_no, 4)
        self.assertEqual(debugger.last_step_no, 2)

        debugger.run()
        self.assertEqual(debugger.current_step_no, len(debugger.steps))
        self.assertEqual(debugger.current_step, debugger.steps[-1])
        self.assertIsNone(debugger.next_step_no)
        self.assertIsNone(debugger.next_step)
        self.assertEqual(debugger.last_step_no, len(debugger.steps) - 1)
        self.assertEqual(debugger.left_step_count, 0)
        self.assertEqual(debugger.last_step, debugger.steps[-2])

        self.assertListEqual(
            debugger.sql("select * from test_result order by id").collect(),
            [
                (1, "a"),
                (2, "b"),
                (3, "c"),
            ],
        )

        self.assertGreater(len(debugger.vars), 0)
        vars_count = len(debugger.vars)
        debugger.add_vars(None)  # will print info and do nothing
        debugger.add_vars({"__some_var": 1})
        self.assertEqual(len(debugger.vars), vars_count + 1)
        debugger.set_vars({"__some_var": 1})
        self.assertEqual(len(debugger.vars), vars_count + 1)  # vars will not change when finished
        self.assertEqual(len(debugger.initial_vars), 1)
        debugger.set_vars(variables)
        self.assertGreater(len(debugger.templates), 0)
        debugger.showdf("test_result")

        debugger.step_to(-1)  # will print info and do nothing

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

        self.assertListEqual(
            debugger.sql("select * from test_result order by id").collect(),
            [
                (1, "a"),
                (2, "b"),
                (3, "c"),
            ],
        )
        debugger.report()
