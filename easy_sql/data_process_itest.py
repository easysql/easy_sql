import os.path
import unittest
from typing import List, Optional

from easy_sql import data_process

proj_base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _data_process(
    sql_file: str,
    vars: Optional[str],
    dry_run: Optional[str],
    print_command: bool,
    backend_config: Optional[List[str]] = None,
) -> None:
    data_process.EasySqlProcessor(sql_file, vars, dry_run, print_command).process(backend_config)


class DataProcessTest(unittest.TestCase):
    def test_spark(self):
        _data_process(os.path.join(proj_base_dir, "test/sample_etl.spark.sql"), None, None, False)

    def test_postgres(self):
        _data_process(os.path.join(proj_base_dir, "test/sample_etl.postgres.sql"), None, None, False)

    def test_clickhouse(self):
        _data_process(os.path.join(proj_base_dir, "test/sample_etl.clickhouse.sql"), None, None, False)

    def test_flink_postgres(self):
        print(_data_process(os.path.join(proj_base_dir, "test/sample_etl.flink.postgres.sql"), None, None, False))
