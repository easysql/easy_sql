import re
import unittest

from easy_sql.config.sql_config import (
    EasySqlConfig,
    FlinkBackendConfig,
    SparkBackendConfig,
)


class EasySqlConfigTest(unittest.TestCase):
    def test_extra_config(self):
        config = EasySqlConfig.from_sql(
            sql="""
-- config: easy_sql.udf_file_path=test/sample_data_process.py
-- config: easy_sql.spark_submit=/my/custom/spark-submit
-- config: spark.test=1
-- config: spark.files=test/sample_etl.spark.sql,test/sample_etl.postgres.sql,
        """,
            sql_file="",
            extra_config=["easy_sql.udf_file_path=test/sample_etl.spark.sql", "spark.test=2"],
        )
        self.assertEqual(config.udf_file_path, "test/sample_etl.spark.sql")
        self.assertEqual(
            config.customized_backend_conf,
            ["spark.test=1", "spark.files=test/sample_etl.spark.sql,test/sample_etl.postgres.sql,", "spark.test=2"],
        )

        config.update_default_easy_sql_conf(
            [
                "easy_sql.udf_file_path=test/sample_etl.spark.sql",
                "easy_sql.scala_udf_initializer=abc",
                "easy_sql.func_file_path=test/sample_data_process.py",
                "spark.test=2",
            ]
        )
        self.assertEqual(config.udf_file_path, "test/sample_etl.spark.sql")
        self.assertEqual(config.func_file_path, "test/sample_data_process.py")
        self.assertEqual(config.scala_udf_initializer, "abc")

    def test_parse_spark_config(self):
        _config = EasySqlConfig.from_sql(
            sql="""
-- config: easy_sql.udf_file_path=test/sample_data_process.py
-- config: easy_sql.func_file_path=test/sample_data_process.py
-- config: easy_sql.spark_submit=/my/custom/spark-submit
-- config: spark.test=1
-- config: spark.test=2
-- config: spark.files=test/sample_etl.spark.sql,test/sample_etl.postgres.sql,
        """,
            sql_file="",
        )

        config = SparkBackendConfig(_config, ["spark.master=yarn", "spark.test=2"])
        contains_conf = lambda c: len([_c for _c in config.spark_conf_command_args() if _c.find(c) != -1]) == 1
        find_conf = lambda search_str: [_c for _c in config.spark_conf_command_args() if _c.find(search_str) != -1]
        print(config.spark_conf_command_args())

        self.assertEqual(config.spark_submit, "/my/custom/spark-submit")
        self.assertTrue(contains_conf("spark.master=yarn"))
        self.assertFalse(contains_conf("spark.test=1"))
        self.assertTrue(contains_conf("spark.test=2"))
        self.assertEqual(find_conf("spark.files=")[0].count(","), 3)

    def test_parse_spark_config_with_relative_files(self):
        _config = EasySqlConfig.from_sql(
            sql="""
-- config: easy_sql.udf_file_path=sample_data_process.py
-- config: easy_sql.func_file_path=sample_data_process.py
-- config: spark.files=test/sample_etl.spark.sql,sample_etl.postgres.sql,
        """,
            sql_file="test/sample_etl.spark.sql",
        )

        config = SparkBackendConfig(_config)
        find_conf = lambda search_str: [_c for _c in config.spark_conf_command_args() if _c.find(search_str) != -1]
        print(config.spark_conf_command_args())

        self.assertEqual(find_conf("spark.files=")[0].count(","), 2)

    def test_parse_flink_config(self):
        _config = EasySqlConfig.from_sql(
            sql="""
-- config: easy_sql.udf_file_path=test/sample_data_process.py
-- config: easy_sql.func_file_path=test/sample_data_process.py
-- config: easy_sql.flink_run=/my/custom/flink
-- config: flink.cmd=-py test/sample_data_process.py
-- config: flink.cmd=--pyFiles test/sample_etl.postgres.sql
-- config: flink.cmd=-pym test.a
-- config: flink.python.fn-execution.bundle.size=100
-- config: flink.cmd=-pyarch test/sample_etl.spark.sql,test/sample_etl.postgres.sql,
-- config: flink.a=a
        """,
            sql_file="",
        )

        config = FlinkBackendConfig(
            _config,
            [
                "flink.cmd=--parallelism 2",
                "flink.cmd=-py test/sample_etl.spark.sql",
                "flink.cmd=-pyarch test/sample_etl.clickhouse.sql",
            ],
        )
        contains_conf = lambda c: len([_c for _c in config.flink_conf_command_args() if _c.find(c) != -1]) == 1
        contains_conf_re = (
            lambda re_conf: len([_c for _c in config.flink_conf_command_args() if re.match(re_conf, _c)]) == 1
        )
        find_conf = lambda search_str: [_c for _c in config.flink_conf_command_args() if _c.find(search_str) != -1]
        print(config.flink_conf_command_args())

        self.assertEqual(config.flink, "/my/custom/flink")
        self.assertTrue(contains_conf("--parallelism 2"))
        self.assertFalse(contains_conf("-Da=a"))
        self.assertTrue(contains_conf_re(r"-py .*test/sample_data_process.py"))
        self.assertEqual(find_conf("--pyFiles ")[0].count(","), 2)

    def test_parse_flink_config_with_relative_files(self):
        _config = EasySqlConfig.from_sql(
            sql="""
-- config: easy_sql.udf_file_path=sample_data_process.py
-- config: easy_sql.func_file_path=sample_data_process.py
-- config: flink.cmd=--pyFiles sample_etl.postgres.sql
-- config: flink.cmd=-pyarch sample_etl.spark.sql,test/sample_etl.postgres.sql,
        """,
            sql_file="test/sample_etl.spark.sql",
        )

        config = FlinkBackendConfig(_config)
        find_conf = lambda search_str: [_c for _c in config.flink_conf_command_args() if _c.find(search_str) != -1]
        print(config.flink_conf_command_args())

        self.assertEqual(find_conf("--pyFiles ")[0].count(","), 2)
        self.assertEqual(find_conf("-pyarch ")[0].count(","), 1)

    def test_parse_flink_config_with_jvm_config(self):
        _config = EasySqlConfig.from_sql(
            sql="""
-- config: flink.cmd=-Djobmanager.memory.process.size=512m
        """,
            sql_file="",
        )

        config = FlinkBackendConfig(
            _config,
            [
                "flink.cmd=--parallelism 2",
                "flink.cmd=-Djobmanager.memory.process.size=1024m",
                "flink.cmd=-Dtaskmanager.memory.process.size=1024m",
                "easy_sql.flink_tables_file_path=test/sample_etl.postgres.sql",
            ],
        )
        contains_conf = lambda c: len([_c for _c in config.flink_conf_command_args() if _c == c]) == 1
        print(config.flink_conf_command_args())

        self.assertTrue(contains_conf("--parallelism 2"))
        self.assertTrue(contains_conf("-Djobmanager.memory.process.size=512m"))
        self.assertFalse(contains_conf("-Djobmanager.memory.process.size=1024m"))
        self.assertTrue(contains_conf("-Dtaskmanager.memory.process.size=1024m"))
        self.assertTrue(
            config.flink_tables_file_path is not None
            and config.flink_tables_file_path.endswith("test/sample_etl.postgres.sql")
        )
