import unittest

from easy_sql.sql_processor.backend import Partition
from easy_sql.sql_processor.backend.rdb import ChSqlDialect, SqlExpr


class RdbTest(unittest.TestCase):
    def test_ch_config(self):
        ch_config = ChSqlDialect(SqlExpr(), "dataplat.__table_partitions__")
        sql = ch_config.delete_partition_sql("test.test", [Partition("dt", "20210101")])
        self.assertEqual(
            sql,
            [
                "alter table test.test drop partition tuple('20210101')",
                "alter table dataplat.__table_partitions__ delete "
                "where db_name = 'test' and table_name = 'test' and partition_value = '20210101'",
            ],
        )


if __name__ == "__main__":
    unittest.main()
