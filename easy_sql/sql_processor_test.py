import unittest
from decimal import Decimal
from typing import Dict

from pyspark.sql.types import IntegerType, StructField, StructType
from pyspark.sql.utils import ParseException

from easy_sql.base_test import LocalSpark, run_sql

from .sql_processor import FuncRunner, SqlProcessor, SqlProcessorException, VarsContext
from .sql_processor.step import StepFactory


class SqlProcessorTest(unittest.TestCase):
    def test_process_sql(self):
        sql = """
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
select
    id as id,
    #{type} as type
from target
where
    type = #{type};
-- target=template.test_c
select
    id as id,
    #{type2} as type
from target
where
    type = #{type1};

-- target=broadcast.test_result
select * from (
    @{test_a()}
) -- comment
--
union all
select * from (
    @{test_b(type='b')}
)
union all
select * from (
    @{test_c(type1='c', type2='${c}')}
)
        """
        spark = LocalSpark.get()

        df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")], ["id", "type"])
        df.createOrReplaceTempView("target")

        variables = {"c": "c"}
        funcs = {
            "check": lambda a, b: a and b,
            "f1": lambda a, b: a and b,
            "f2": lambda a, b: a + b,
        }
        self.assertListEqual(
            run_sql(sql, "test_result", funcs=funcs, variables=variables, spark=spark),
            [
                (1, "a"),
                (2, "b"),
                (3, "c"),
            ],
        )

    def test_list_var(self):
        sql = """
-- target=list_variables
select explode(array(1, 2, 3)) as a
-- target=list_variables
select explode(array('1', '2', '3')) as b
        """
        spark = LocalSpark.get()
        processor = SqlProcessor(spark, sql, [], {})
        processor.run()
        self.assertEqual(processor.context.vars_context.list_vars, {"a": [1, 2, 3], "b": ["1", "2", "3"]})

    def test_var_replace(self):
        sql = """
-- target=variables
select 1 as a
, 2 as aa
-- target=variables
select '${f1(${a}, 2)}, ${a}' as b
-- target=variables, if=bool()
select '1' as b
-- target=temp.result, if=f1(,)
select '${b}-${aa}-dont-run' as b
-- target=temp.result
select '${b}-${aa}' as b
        """
        self.assertEqual(run_sql(sql, "result", funcs={"f1": lambda a, b: a + b}), [("12, 1-2",)])

    def test_template_with_special_chars(self):
        # template引用时，参数值如果是字符串，则字符串中不能出现字符`()`，如果有此需求，可以使用变量支持(变量值内部可以有这些符号)
        sql = """
-- target=variables
select 'abc(),' as a,
        '1 as col1, 2 as col2, \\'col-3\\' as col3' as other_cols
-- target=template.templ
select #{a} as a, #{b} as b, #{c} as c, #{other_cols}
-- target=temp.result
@{templ(a='${a}', b=2, c='3', other_cols=${other_cols})}
        """
        self.assertEqual(run_sql(sql, "result", funcs={"c": lambda a, b: a + b}), [("abc(),", 2, "3", 1, 2, "col-3")])

    def test_template_replace_mix_var_replace(self):
        # template中可以有variable引用，解析时会先解析template，再替换variable
        sql = """
-- target=template.temp1
select #{a} as a, #{b} as b, #{c} as c, ${d} as d
-- target=cache.result
@{temp1(a=1, b=2,c=3)}
        """
        self.assertEqual(run_sql(sql, "result", variables={"d": "4"}), [(1, 2, 3, 4)])

        # 可以多次引用template
        sql = """
-- target=template.temp1
#{a} as #{an}, #{b} as #{bn}
-- target=cache.result
select @{temp1(a=1, an=a, b=2, bn=b)},
@{temp1(a=3, an=c, b=4, bn=d)}
        """
        self.assertEqual(run_sql(sql, "result", variables={"d": "4"}), [(1, 2, 3, 4)])

        # 可以在template中引用template, 但是不推荐这样用，会导致逻辑很复杂，理解不易，且容易导致循环引用
        sql = """
-- target=template.temp1
1 as a, #{b} as b
-- target=template.temp2
select @{temp1(b=${b})}
-- target=cache.result
@{temp2}
        """
        self.assertEqual(run_sql(sql, "result", variables={"b": "2"}), [(1, 2)])

        # template引用时，如果没有参数时可以省略括号
        sql = """
-- target=template.templ
select 1 as a
-- target=cache.result
@{templ} union all @{templ()}
        """
        self.assertEqual(run_sql(sql, "result"), [(1,), (1,)])

        # template引用时，参数可以换行
        sql = """
-- target=template.temp1
#{a} as a,
    #{b} as b, #{c} as c, ${d} as d, ${d1(1, ${d})} as e
-- target=cache.result
select
@{temp1(
a=1.1, b='2-2',
    c=3)}
            """
        self.assertEqual(
            run_sql(sql, "result", funcs={"d1": lambda a, b: a + b}, variables={"d": "4"}),
            [(Decimal("1.1"), "2-2", 3, 4, 14)],
        )

        # template引用时，参数值可以是变量
        sql = """
-- target=template.templ
select #{a} as a, #{b} as b, #{c} as c
-- target=temp.result
@{templ(a=${a}, b=2, c=${a})}
        """
        self.assertEqual(run_sql(sql, "result", variables={"a": "1"}), [(1, 2, 1)])

        # template引用时，参数值如果是变量，则变量中不能有函数
        sql = """
-- target=template.templ
select #{a} as a, #{b} as b, #{c} as c
-- target=temp.result
@{templ(a=${a}, b=2, c=${c(1, 2)})}
        """
        with self.assertRaises(expected_exception=ParseException):
            run_sql(sql, "result", funcs={"c": lambda a, b: a + b}, variables={"a": "1"})

        # 可以调用一个函数
        sql = """
-- target=func.t(a, ${b})
-- target=temp.result
select 1 as a
            """
        c = ""

        def func(a, b):
            nonlocal c
            c = a + b

        self.assertEqual(run_sql(sql, "result", funcs={"t": func}, variables={"b": "2"}), [(1,)])
        self.assertEqual(c, "a2")

        # dryrun运行时，只会打印日志，不会真的写入hive库
        sql = """
-- target=temp.result
select 1 as a, 2 as b
-- target=hive.db.result
select 1 as a, 2 as b
            """
        self.assertEqual(run_sql(sql, "result", dry_run=True), [(1, 2)])

        # skip_all生效时，剩下的步骤均不会执行
        sql = """
-- target=variables
select '${f(0)}' as __skip_all__
-- target=temp.result
select 1 as a, 2 as b
-- target=variables
select '${f(1)}' as __skip_all__
-- target=temp.result
select 11 as a, 22 as b
            """
        self.assertEqual(run_sql(sql, "result", funcs={"f": lambda x: x == "1"}), [(1, 2)])

        # 可以给函数传入元变量，如 __step__
        sql = """
-- target=func.ensure_partition_exists(${__step__}, a, b, c, 1)
-- target=temp.result
select 1, 2 as dt
-- target=func.ensure_no_null_data_in_table(${__step__}, result, dt=${d})
"""
        self.assertEqual(run_sql(sql, "result", variables={"d": "1"}), [(1, 2)])

    def test_should_be_able_to_parse_target_when_target_is_at_last(self):
        sql = """
-- target=temp.result
select 1 as a
-- target=func.t(a, ${b})
-- target=func.t(a, ${b})"""
        c = 0

        def func(a, b):
            nonlocal c
            c += 1

        self.assertEqual(run_sql(sql, "result", funcs={"t": func}, variables={"b": "2"}), [(1,)])
        self.assertEqual(c, 2)

    def test_should_process_exception_when_exception_handler_not_null(self):
        sql = """
-- target=variables
select 'old_a' as var_a

-- case1: exception_handler 可以带参数，使用 `{var_name}` 来引用变量，变量值取 handler 被执行时的取值
-- target=variables
select "exception_handler1({__step__}, {var_a}, b)" as __exception_handler__
-- target=variables
select 'current_a' as var_a
-- target=temp.res1
select ${not_existed1}

-- case2: exception_handler 可以不带参数
-- target=variables
select 'exception_handler2()' as __exception_handler__
-- target=temp.res2
select ${not_existed2}

-- case3: 可以针对接下来的步骤取消 exception_handler 设定，将值置为 null 即可
-- target=variables
select null as __exception_handler__
-- target=temp.res3
select ${not_existed3}
"""
        from .sql_processor import SqlProcessor, StepReport, StepStatus
        from .sql_processor.step import Step

        def exception_handler1(step: Step, a, b):
            return lambda e: step.collect_report(status=StepStatus.FAILED, message=f"error: {a}, {b}, {e}")

        def exception_handler2():
            return lambda e: print(f"error: {e}")

        spark = LocalSpark.get()
        processor = SqlProcessor(spark, sql)
        processor.register_funcs({"exception_handler1": exception_handler1, "exception_handler2": exception_handler2})

        self.assertRaisesRegex(Exception, ".*not_existed3.*", processor.run)

        assert processor.reporter.step_reports is not None
        reports: Dict[str, StepReport] = processor.reporter.step_reports
        self.assertIn("error: current_a, b, ", reports["step-4"].report_as_text(1))
        self.assertEqual(StepStatus.FAILED, reports["step-4"].status)
        self.assertEqual(StepStatus.FAILED, reports["step-6"].status)

    def test_should_raise_exception_when_check_failed(self):
        check_fail_sqls = [
            "-- target=check.test_check\nselect 1 as actual, 0 as expected",
            "-- target=check.test_check\nselect 1 as actual, 1 as expected1",
            "-- target=check.test_check\nselect 1 as actual, 0 as expected where 1=0",
        ]
        for sql in check_fail_sqls:
            processor = SqlProcessor(LocalSpark.get(), sql)
            self.assertRaises(SqlProcessorException, processor.run)

    def test_should_not_fail_and_log_no_data_when_no_data_found_in_log_target(self):
        processor = SqlProcessor(LocalSpark.get(), "-- target=log.no_data\nselect 1 as actual, 0 as expected where 1=0")
        processor.run()

    def test_should_raise_error_when_no_table_and_no_create_output_table(self):
        processor = SqlProcessor(LocalSpark.get(), "-- target=output.t.some_table\nselect 1 as a, 0 as b")
        self.assertRaises(Exception, lambda: processor.run())

    def test_should_raise_error_when_output_table_name_does_not_contain_dbname(self):
        processor = SqlProcessor(LocalSpark.get(), "-- target=output.some_table\nselect 1 as actual, 0 as expected")
        self.assertRaises(Exception, lambda: processor.run())

    def test_should_do_action_step_ok(self):
        processor = SqlProcessor(
            LocalSpark.get(),
            (
                "-- target=variables\nselect true as __create_output_table__\n"
                "-- target=output.t.some_table\nselect 1 as a, 0 as b\n"
                "-- target=action.some_action\ndrop table t.some_table"
            ),
        )
        processor.run()
        from easy_sql.sql_processor.backend import TableMeta

        self.assertFalse(processor.backend.table_exists(TableMeta("t.some_table")))

    def test_should_add_static_partition_value_for_dryrun_as_well(self):
        sql = """
-- target=variables
select 20200101 as __partition__dt
-- target=output.t.result
select 1 as a, 2 as b
        """
        spark = LocalSpark.get()
        processor = SqlProcessor(spark, sql)
        processor.run(True)
        result = [t for t in processor.backend.temp_tables() if t.startswith("result_") and t.endswith("_output")]
        self.assertEqual(len(result), 1)
        self.assertEqual(spark.sql(f"select * from {result[0]}").collect(), [(1, 2, "20200101")])


class FuncRunnerTest(unittest.TestCase):
    def test_fun_func(self):
        def f1(a, b) -> bool:
            return a and b

        self.assertFalse(FuncRunner({"f1": f1}).run_func("f1(,)", VarsContext()))
        self.assertTrue(FuncRunner({"f1": f1}).run_func("f1(1,1)", VarsContext()))
        self.assertTrue(FuncRunner({"f1": f1}).run_func("f1(1,${a})", VarsContext({"a": 1})))
        self.assertFalse(FuncRunner({"f1": f1}).run_func("f1(1,${a})", VarsContext({"a": ""})))

    def test_ensure_dwd_partition_exists(self):
        spark = LocalSpark.get()
        spark.createDataFrame(
            [], StructType([StructField("id", IntegerType()), StructField("pt", IntegerType())])
        ).write.mode("overwrite").partitionBy("pt").saveAsTable("empty_table")
        spark.createDataFrame(
            [
                (
                    1,
                    None,
                    None,
                    20210101,
                ),
                (
                    1,
                    "1",
                    "1",
                    20210102,
                ),
            ],
            ["id", "fk1", "fk2", "pt"],
        ).write.mode("overwrite").partitionBy("pt").saveAsTable("data_table")
        spark.createDataFrame(
            [
                (
                    1,
                    "1",
                    20210101,
                )
            ],
            ["id", "fk", "pt"],
        ).write.mode("overwrite").partitionBy(
            "pt"
        ).saveAsTable("data_table1")
        # failure case1: 被检测表是空的
        processor = SqlProcessor(
            spark, "-- target=check.ensure_dwd_partition_exists(${__step__}, empty_table, 20210101)", [], {}
        )
        with self.assertRaises(Exception):  # noqa: B017
            processor.run(dry_run=True)
        assert processor.reporter.step_reports is not None
        self.assertIn("partition 20210101 not exists: empty_table", processor.reporter.step_reports["step-1"].messages)

        # failure case2: 检测分区在被检测表中确实不存在
        processor = SqlProcessor(
            spark, "-- target=check.ensure_dwd_partition_exists(${__step__}, data_table, 20210103)", [], {}
        )
        with self.assertRaises(Exception):  # noqa: B017
            processor.run(dry_run=True)
        assert processor.reporter.step_reports is not None
        self.assertIn("partition 20210103 not exists: data_table", processor.reporter.step_reports["step-1"].messages)

        # happy case1: 检测分区存在
        processor = SqlProcessor(
            spark, "-- target=check.ensure_dwd_partition_exists(${__step__}, data_table, 20210101)", [], {}
        )
        processor.run(dry_run=True)

        # happy case2: 检测分区 < first_partition
        processor = SqlProcessor(
            spark, "-- target=check.ensure_dwd_partition_exists(${__step__}, data_table, 20201201)", [], {}
        )
        processor.run(dry_run=True)

        # failure case: 检测分区存在，但全部外键均为空
        processor = SqlProcessor(
            spark,
            "-- target=check.ensure_dwd_partition_exists(${__step__}, data_table, 20210101, fk1, fk2)",
            [],
            {},
        )
        with self.assertRaises(Exception):  # noqa: B017
            processor.run(dry_run=True)
        assert processor.reporter.step_reports is not None
        self.assertIn(
            "all fk cols are null in partition: table_name=data_table, partition=20210101",
            processor.reporter.step_reports["step-1"].messages,
        )

        # failure case: 检测分区 < first_partition，全部外键均为空
        processor = SqlProcessor(
            spark,
            "-- target=check.ensure_dwd_partition_exists(${__step__}, data_table, 20201101, fk1, fk2)",
            [],
            {},
        )
        with self.assertRaises(Exception):  # noqa: B017
            processor.run(dry_run=True)
        assert processor.reporter.step_reports is not None
        self.assertIn(
            "all fk cols are null in partition: table_name=data_table, partition=20210101",
            processor.reporter.step_reports["step-1"].messages,
        )

        # happy case: 检测分区存在，且存在外键不为空的数据
        processor = SqlProcessor(
            spark, "-- target=check.ensure_dwd_partition_exists(${__step__}, data_table, 20210102, fk1, fk2)", [], {}
        )
        processor.run(dry_run=True)

        # happy case: 检测分区 < first_partition，且存在外键不为空的数据
        processor = SqlProcessor(
            spark, "-- target=check.ensure_dwd_partition_exists(${__step__}, data_table1, 20201201, fk)", [], {}
        )
        processor.run(dry_run=True)

    def test_ensure_partition_or_first_partition_exists(self):
        spark = LocalSpark.get()
        spark.createDataFrame(
            [], StructType([StructField("id", IntegerType()), StructField("pt", IntegerType())])
        ).write.mode("overwrite").partitionBy("pt").saveAsTable("empty_table")
        spark.createDataFrame(
            [
                (
                    1,
                    20210101,
                )
            ],
            ["id", "pt"],
        ).write.mode("overwrite").partitionBy(
            "pt"
        ).saveAsTable("data_table")
        # failure case1: 被检测表是空的
        processor = SqlProcessor(
            spark,
            "-- target=check.ensure_partition_or_first_partition_exists(${__step__}, empty_table, 20210101)",
            [],
            {},
        )
        with self.assertRaises(Exception):  # noqa: B017
            processor.run(dry_run=True)

        assert processor.reporter.step_reports is not None
        self.assertIn(
            "partition 20210101 not exists: ['empty_table']", processor.reporter.step_reports["step-1"].messages
        )

        # failure case2: 检测分区在被检测表中确实不存在
        processor = SqlProcessor(
            spark,
            "-- target=check.ensure_partition_or_first_partition_exists(${__step__}, data_table, 20210102)",
            [],
            {},
        )
        with self.assertRaises(Exception):  # noqa: B017
            processor.run(dry_run=True)
        assert processor.reporter.step_reports is not None
        self.assertIn(
            "partition 20210102 not exists: ['data_table']", processor.reporter.step_reports["step-1"].messages
        )

        # failure case3: 检测多张表，message 中应该只有检查失败的表
        processor = SqlProcessor(
            spark,
            (
                "-- target=check.ensure_partition_or_first_partition_exists(${__step__}, empty_table, data_table,"
                " 20210101)"
            ),
            [],
            {},
        )
        with self.assertRaises(Exception):  # noqa: B017
            processor.run(dry_run=True)
        assert processor.reporter.step_reports is not None
        self.assertIn(
            "partition 20210101 not exists: ['empty_table']", processor.reporter.step_reports["step-1"].messages
        )

        # happy case1: 检测分区存在
        processor = SqlProcessor(
            spark,
            "-- target=check.ensure_partition_or_first_partition_exists(${__step__}, data_table, 20210101)",
            [],
            {},
        )
        processor.run(dry_run=True)

        # happy case2: 检测分区 < first_partition
        processor = SqlProcessor(
            spark,
            "-- target=check.ensure_partition_or_first_partition_exists(${__step__}, data_table, 20201201)",
            [],
            {},
        )
        processor.run(dry_run=True)


test_snippet = """
-- target=variables
select 1 as a
"""
test_snippet2 = """
-- target=variables
select 1 as b
"""


class StepFactoryTest(unittest.TestCase):
    def test_include_snippet_from_py(self):
        spark = LocalSpark.get()
        sql = """
-- can have comments here
-- include=easy_sql.sql_processor_test.test_snippet
-- target=temp.result1
select ${a} as res_a

-- can add include anywhere
-- include=easy_sql.sql_processor_test.test_snippet2
-- target=temp.result2
select ${b} as res_b
        """

        steps = StepFactory(None, None).create_from_sql(sql)  # type: ignore

        self.assertEqual(4, len(steps))
        self.assertEqual(run_sql(sql, "result1", spark=spark), [(1,)])
        self.assertEqual(run_sql(sql, "result2", spark=spark), [(1,)])

    def test_include_snippet_from_sql(self):
        sql = """
-- can have comments here
-- include=/tmp/test_snippet.sql
-- target=temp.result1
select ${a} as res_a

-- can add include anywhere
-- include=/tmp/test_snippet2.sql
-- target=temp.result2
select ${b} as res_b
        """
        with open("/tmp/test_snippet.sql", "w") as f:
            f.write(test_snippet)
        with open("/tmp/test_snippet2.sql", "w") as f:
            f.write(test_snippet2)

        spark = LocalSpark.get()
        steps = StepFactory(None, None).create_from_sql(sql)  # type: ignore

        self.assertEqual(4, len(steps))
        self.assertEqual(run_sql(sql, "result1", spark=spark), [(1,)])
        self.assertEqual(run_sql(sql, "result2", spark=spark), [(1,)])
