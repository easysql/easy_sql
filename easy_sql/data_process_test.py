import os.path
import unittest
from typing import Optional

from easy_sql import data_process

proj_base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _data_process(sql_file: str, vars: Optional[str], dry_run: Optional[str], print_command: bool) -> Optional[str]:
    return data_process.EasySqlProcessor(sql_file, vars, dry_run, print_command).process()


class DataProcessTest(unittest.TestCase):
    def test_spark(self):
        command = _data_process(os.path.join(proj_base_dir, "test/sample_etl.spark.sql"), None, None, True)
        assert command is not None
        print(command)
        self.assertRegex(
            command,
            r"spark-submit --conf spark.master=local\[2\] --conf spark.submit.deployMode=client "
            r"--conf spark.app.name=sample_etl.spark_[\d]+ "
            "--conf spark.sql.warehouse.dir=/tmp/spark-warehouse-localdw "
            '--conf spark.driver.extraJavaOptions="-Dderby.system.home=/tmp/spark-warehouse-metastore '
            '-Dderby.stream.error.file=/tmp/spark-warehouse-metastore.log" '
            '--conf spark.files="[^"]+test/sample_etl.spark.sql" '
            '"[^"]+/easy_sql/data_process.py" '
            "-f .+/test/sample_etl.spark.sql --dry-run 0",
        )

    def test_flink_hive(self):
        command = _data_process(os.path.join(proj_base_dir, "test/sample_etl.flink.hive.sql"), None, None, True)

        assert command is not None
        self.assertRegex(
            command,
            r".*flink run --parallelism 2 "
            '--pyFiles [^"]+test/sample_etl.flink.hive.sql '
            "-t local "
            '--python "[^"]+/easy_sql/data_process.py" '
            "-f .+/test/sample_etl.flink.hive.sql --dry-run 0",
        )

    def test_flink_hive_postgres(self):
        command = _data_process(
            os.path.join(proj_base_dir, "test/sample_etl.flink.hive.postgres.sql"), None, None, True
        )
        assert command is not None
        self.assertRegex(
            command,
            r".*flink run --parallelism 1 "
            '--pyFiles [^"]+test/sample_etl.flink.hive.postgres.sql '
            '--python "[^"]+/easy_sql/data_process.py" '
            "-f .+/test/sample_etl.flink.hive.postgres.sql --dry-run 0",
        )

    def test_flink_scala_udf(self):
        command = _data_process(os.path.join(proj_base_dir, "test/udf/flink-scala/etl_with_udf.sql"), None, None, True)
        assert command is not None
        self.assertRegex(
            command,
            r".*flink run --parallelism 1 "
            '--pyFiles [^"]+test/udf/flink-scala/etl_with_udf.sql --jarfile udf.jar '
            '--python "[^"]+/easy_sql/data_process.py" '
            "-f .+/test/udf/flink-scala/etl_with_udf.sql --dry-run 0",
        )
