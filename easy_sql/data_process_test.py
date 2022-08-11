import unittest

from easy_sql.data_process import EasySqlConfig


class EasySqlConfigTest(unittest.TestCase):
    def test_parse_config(self):
        config = EasySqlConfig.from_sql(
            sql="""
-- config: easy_sql.udf_file_path=test/sample_data_process.py
-- config: easy_sql.func_file_path=test/sample_data_process.py
-- config: easy_sql.spark_submit=/my/custom/spark-submit
-- config: spark.files=test/sample_etl.spark.sql,test/sample_etl.postgres.sql,
        """,
            sql_file="",
        )
        self.assertEqual(config.spark_submit, "/my/custom/spark-submit")
        print(config.spark_conf_command_args())
        self.assertEqual(config.spark_conf_command_args()[-1].count(","), 3)
        self.assertEqual(config.spark_submit, "/my/custom/spark-submit")
