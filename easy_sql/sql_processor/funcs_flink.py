from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from easy_sql.logger import logger

from .funcs_common import AlertFunc, AnalyticsFuncs, ColumnFuncs, TableFuncs
from .funcs_common import TestFuncs as BaseTestFuncs

if TYPE_CHECKING:
    from easy_sql.sql_processor.step import Step

    from .backend import FlinkBackend


__all__ = [
    "StreamingFuncs",
    "ColumnFuncs",
    "ParallelismFuncs",
    "AlertFunc",
    "TableFuncs",
    "AnalyticsFuncs",
    "TestFuncs",
]


class ParallelismFuncs:
    def __init__(self, flink: FlinkBackend):
        self.flink = flink

    def set_parallelism(self, partitions: str):
        try:
            int(partitions)
        except ValueError:
            raise Exception(f"partitions must be an int when repartition a table, got `{partitions}`")
        self.flink.flink.get_config().set("table.exec.resource.default-parallelism", partitions)


class StreamingFuncs:
    def __init__(self, flink: FlinkBackend):
        self.flink = flink

    def execute_streaming_inserts(self):
        self.flink.execute_streaming_inserts()


class TestFuncs(BaseTestFuncs):
    def __init__(self, flink: FlinkBackend):
        super().__init__(flink)
        self.flink = flink

    def exec_sql_in_source(self, step: Step, db: str, connector: str):
        from easy_sql.utils.db_connection_utils import (
            get_connector_raw_conn_for_flink_backend,
        )

        if step.select_sql:
            conn = get_connector_raw_conn_for_flink_backend(self.flink, db, connector)
            if not conn:
                raise Exception(
                    f"Cannot build a suitable coonnection for db {db} and connector {connector}. Please ensure it is"
                    " configured properly."
                )
            sql_list = [
                sql.strip() for sql in step.select_sql.split("\n") if sql.strip() and not sql.strip().startswith("--")
            ]
            with conn.begin():
                for sql in sql_list:
                    logger.info(f"exec sql in connector {db}.{connector}: {sql}")
                    conn.execute(sql)
            conn.close()

    def test_run_etl(self, config: Any, etl_file: str):
        from easy_sql.config.sql_config import EasySqlConfig
        from easy_sql.utils.flink_test_cluster import FlinkTestClusterManager
        from easy_sql.utils.io_utils import resolve_file

        def run_etl(etl_file: str):
            import subprocess

            command = f'python3 -m easy_sql.data_process -f "{etl_file}" -p'
            logger.info(f"will exec command: {command}")
            command = subprocess.check_output(["bash", "-c", command])
            logger.info(f"will exec command: {command}")
            subprocess.check_call(["bash", "-c", command])

        if config:
            if not isinstance(config, EasySqlConfig):
                raise Exception("config must be an instance of EasySqlConfig")
            relative_to = config.abs_sql_file_path
            print("relative to: ", relative_to)
        else:
            relative_to = ""

        etl_file = resolve_file(etl_file, abs_path=True, relative_to=relative_to)
        with open(etl_file, "r") as f:
            etl_content = f.readlines()
        is_streaming = any(
            [re.match(r"^-- config:\s*easy_sql\.etl_type\s*=\s*streaming", line) for line in etl_content]
        )

        if is_streaming:
            fm = FlinkTestClusterManager()

            originally_started = True
            if fm.is_not_started():
                originally_started = False
                fm.start_cluster()

            try:
                run_etl(etl_file)
            finally:
                if not originally_started:
                    fm.stop_cluster()
        else:
            run_etl(etl_file)
