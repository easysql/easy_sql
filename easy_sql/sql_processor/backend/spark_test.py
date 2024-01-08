import unittest

from pyspark.sql.functions import expr, lit

from easy_sql.base_test import LocalSpark
from easy_sql.sql_processor.backend import SparkTable
from easy_sql.sql_processor.backend.base import TableMeta
from easy_sql.sql_processor.backend.spark import SparkBackend
from easy_sql.sql_processor.common import SqlProcessorException


class SparkTest(unittest.TestCase):
    def test_with_column(self):
        spark = LocalSpark.get()
        df = spark.sql("select 1 as id")
        # expr('2021-01-01') 效果就是 select 2021-01-01，少了引号，解析就会出现奇怪的结果
        self.assertNotEqual(
            SparkTable(df).with_column("data_date", "2021-01-01").df.select("data_date").limit(1).collect(),
            [("2021-01-01",)],
        )
        # expr("'2021-01-01'") 效果就是 select '2021-01-01'，结果正确
        self.assertEqual(
            SparkTable(df).with_column("data_date", "'2021-01-01'").df.select("data_date").limit(1).collect(),
            [("2021-01-01",)],
        )
        # 可以直接传入 Column
        self.assertEqual(
            SparkTable(df).with_column("data_date", lit("2021-01-01")).df.select("data_date").limit(1).collect(),
            [("2021-01-01",)],
        )
        self.assertEqual(SparkTable(df).with_column("flag", "1==2").df.select("flag").limit(1).collect(), [(False,)])
        self.assertEqual(
            SparkTable(df).with_column("flag", expr("1==2")).df.select("flag").limit(1).collect(), [(False,)]
        )

    def test_verify_schema(self):
        spark = LocalSpark.get()
        backend = SparkBackend(spark)
        spark.sql('create table test_verify_schema using parquet as select 1 as id, "a" as name')

        # should check if target table exists
        spark.sql("select 1 as id").createOrReplaceTempView("test_verify_schema0")
        with self.assertRaises(SqlProcessorException):
            backend.verify_schema(TableMeta("test_verify_schema0"), TableMeta("test_verify_schema1"))

        # should verify column name
        spark.sql("select 1 as id").createOrReplaceTempView("test_verify_schema1")
        with self.assertRaises(SqlProcessorException):
            backend.verify_schema(TableMeta("test_verify_schema1"), TableMeta("test_verify_schema"))

        # should ignore case and not verify type
        spark.sql("select 1 as Id, 1 as name").createOrReplaceTempView("test_verify_schema2")
        # should not raise exception
        backend.verify_schema(TableMeta("test_verify_schema2"), TableMeta("test_verify_schema"))

        # should verify type and raise error
        spark.sql("select 1 as id, 1 as Name").createOrReplaceTempView("test_verify_schema21")
        with self.assertRaises(SqlProcessorException):
            backend.verify_schema(TableMeta("test_verify_schema21"), TableMeta("test_verify_schema"), True)

        # should ignore extra column
        spark.sql("select 1 as id, 'a' as name, 1 as id1").createOrReplaceTempView("test_verify_schema3")
        # should not raise exception
        backend.verify_schema(TableMeta("test_verify_schema3"), TableMeta("test_verify_schema"))


if __name__ == "__main__":
    unittest.main()
