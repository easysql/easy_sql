import os.path
import unittest

from easy_sql import data_process

proj_base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class DataProcessTest(unittest.TestCase):
    def test_spark(self):
        command = data_process._data_process(os.path.join(proj_base_dir, "test/sample_etl.spark.sql"), None, None, True)
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
        data_process._data_process(os.path.join(proj_base_dir, "test/sample_etl.spark.sql"), None, None, False)

    def test_postgres(self):
        data_process._data_process(os.path.join(proj_base_dir, "test/sample_etl.postgres.sql"), None, None, False)

    def test_clickhouse(self):
        data_process._data_process(os.path.join(proj_base_dir, "test/sample_etl.clickhouse.sql"), None, None, False)
