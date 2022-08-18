import unittest

from . import sql_test


class SqlTestTest(unittest.TestCase):
    def test_convert_test_for_spark(self):
        self.run_test("spark")

    def test_convert_test_for_postgres(self):
        self.run_test("postgres")

    def test_convert_test_for_clickhouse(self):
        self.run_test("clickhouse")

    def run_test(self, backend: str):
        sql_test._convert_json(f"test/sample_etl.{backend}.xlsx")
        sql_test._run_test(f"test/sample_etl.{backend}.xlsx", backend=backend)
        sql_test._run_test(f"test/sample_etl.{backend}.json", backend=backend)
