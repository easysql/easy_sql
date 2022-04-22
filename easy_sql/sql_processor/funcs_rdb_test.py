import unittest

from easy_sql.sql_processor.backend.rdb import RdbBackend
from easy_sql.sql_processor.funcs_rdb import PartitionFuncs


class FuncsRdbTest(unittest.TestCase):

    @unittest.skip('integration test')
    def test_partitionfuncs_is_first_partition(self):
        self.backend = RdbBackend('postgresql://postgres:123456@testpg:15432/postgres')
        pf = PartitionFuncs(self.backend)
        print(pf._get_partition_values('ods_sales.sales_dealer'))
