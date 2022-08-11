import unittest

from pyspark.sql.functions import expr, lit

from easy_sql.base_test import LocalSpark
from easy_sql.sql_processor.backend import SparkTable


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


if __name__ == "__main__":
    unittest.main()
