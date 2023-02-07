import unittest

from easy_sql.base_test import TEST_PG_JDBC_PASSWD, TEST_PG_JDBC_URL, TEST_PG_JDBC_USER
from easy_sql.config.sql_config import EasySqlConfig
from easy_sql.sql_processor.backend import FlinkBackend, FlinkTablesConfig
from easy_sql.sql_processor.step import Step
from easy_sql.utils.flink_test_cluster import FlinkTestClusterManager

from .funcs_flink import TestFuncs


def step_with_sql(sql: str) -> Step:
    return Step("0", None, None, select_sql=sql)  # type: ignore


class FlinkFuncsTest(unittest.TestCase):
    def create_flink_backend(self):
        return FlinkBackend(
            True,
            FlinkTablesConfig(
                {
                    "databases": [
                        {
                            "name": "db",
                            "connectors": [
                                {
                                    "name": "jdbc",
                                    "options": {
                                        "connector": "jdbc",
                                        "url": TEST_PG_JDBC_URL,
                                        "username": TEST_PG_JDBC_USER,
                                        "password": TEST_PG_JDBC_PASSWD,
                                    },
                                }
                            ],
                        }
                    ]
                }
            ),
        )

    def test_exec_sql_in_source(self):
        fb = self.create_flink_backend()
        tf = TestFuncs(fb)
        tf.exec_sql_in_source(step_with_sql("select 1;\nselect now();"), "db", "jdbc")

    def test_run_etl_streaming(self):
        fb = self.create_flink_backend()
        tf = TestFuncs(fb)
        with open("/tmp/flink_func_test__test_run_etl.sql", "w") as f:
            f.write(
                """
-- backend: flink
-- config: easy_sql.etl_type=streaming
-- config: flink.cmd=-pyexec python3
-- config: flink.cmd=-t remote
-- config: flink.cmd=-pyclientexec python3
-- target=variables
select
    'append'           as __save_mode__
            """
            )
        fm = FlinkTestClusterManager()
        if fm.is_not_started():
            fm.start_cluster()
        tf.test_run_etl(None, "/tmp/flink_func_test__test_run_etl.sql")
        self.assertTrue(fm.is_started())
        fm.stop_cluster()

        tf.test_run_etl(None, "/tmp/flink_func_test__test_run_etl.sql")
        self.assertTrue(fm.is_not_started())

    def test_run_etl_batch(self):
        fb = self.create_flink_backend()
        tf = TestFuncs(fb)
        with open("/tmp/flink_func_test__test_run_etl.sql", "w") as f:
            f.write(
                """
-- backend: flink
-- config: easy_sql.etl_type=batch
-- config: flink.cmd=-pyexec python3
-- config: flink.cmd=-t local
-- config: flink.cmd=-pyclientexec python3
-- target=variables
select
    'append'           as __save_mode__
            """
            )
        tf.test_run_etl(
            EasySqlConfig.from_sql(sql_file="/tmp/flink_func_test__test_run_etl.sql"),
            "/tmp/flink_func_test__test_run_etl.sql",
        )
