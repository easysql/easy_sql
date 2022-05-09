import unittest
from typing import Tuple

from easy_sql.base_test import TEST_PG_URL, TEST_CH_URL
from easy_sql.sql_processor import Step, FuncRunner
from easy_sql.sql_processor.backend import Partition
from easy_sql.sql_processor.backend.base import Col, Backend
from easy_sql.sql_processor.backend.rdb import RdbBackend
from easy_sql.sql_processor.context import ProcessorContext, VarsContext, TemplatesContext
from easy_sql.sql_processor.funcs_common import ColumnFuncs, TableFuncs, AlertFunc, Alerter
from easy_sql.sql_processor.funcs_rdb import PartitionFuncs
from easy_sql.sql_processor.step import ReportCollector


class FuncsRdbTest(unittest.TestCase):

    def test_funcs_pg(self):
        from easy_sql.sql_processor.backend.postgres import PostgresBackend
        backend = PostgresBackend(TEST_PG_URL)
        int_type, str_type, pt_type = 'int', 'text', 'text'
        self.run_test(backend, (int_type, str_type, pt_type))

    def test_funcs_ch(self):
        from easy_sql.sql_processor.backend.clickhouse import ChBackend
        backend = ChBackend(TEST_CH_URL)
        int_type, str_type, pt_type = 'Nullable(Int32)', 'String', 'String'
        self.run_test(backend, (int_type, str_type, pt_type))

    def run_test(self, backend: RdbBackend, types: Tuple[str, str, str]):
        try:
            self._run_test(backend, types)
        finally:
            backend.clean()

    def _run_test(self, backend: RdbBackend, types: Tuple[str, str, str]):
        int_type, str_type, pt_type = types
        backend.exec_native_sql(backend.sql_dialect.drop_db_sql('t'))
        table_name = 't.func_test'
        backend.create_table_with_data(table_name,
                                       [['1', 1, '2022-01-01'], ['2', None, '2022-01-02']],
                                       [Col('id', str_type), Col('val', int_type), Col('pt', pt_type)],
                                       [Partition(field='pt')])

        pf = PartitionFuncs(backend)

        class _ReportCollector(ReportCollector):
            def collect_report(self, step: 'Step', status: str = None, message: str = None):
                pass

        step = Step('1', _ReportCollector(), FuncRunner({'bool': lambda x: x == '1'}),
                    select_sql='select 0 as a')

        self.assertTrue(pf.is_first_partition(table_name, '2022-01-01'))
        self.assertTrue(pf.is_not_first_partition(table_name, '2022-01-02'))
        self.assertTrue(pf.partition_exists(table_name, '2022-01-02'))
        self.assertFalse(pf.partition_exists(table_name, '2022-01-03'))
        self.assertTrue(pf.partition_not_exists(table_name, '2022-01-03'))
        self.assertTrue(pf.ensure_partition_or_first_partition_exists(step, table_name, '2022-01-01'))
        self.assertTrue(pf.ensure_partition_or_first_partition_exists(step, table_name, '2022-01-02'))
        self.assertTrue(pf.ensure_partition_exists(step, table_name, '2022-01-01'))
        self.assertFalse(pf.ensure_partition_exists(step, table_name, '2022-01-03'))
        self.assertTrue(pf.ensure_dwd_partition_exists(step, table_name, '2022-01-01', 'id'))
        self.assertFalse(pf.ensure_dwd_partition_exists(step, table_name, '2022-01-02', 'val'))
        self.assertEqual(pf.get_partition_or_first_partition(table_name, '2021-01-02'), '2022-01-01')
        self.assertEqual(pf.get_partition_or_first_partition(table_name, '2022-01-02'), '2022-01-02')
        self.assertEqual(pf.get_first_partition_optional(table_name), '2022-01-01')
        self.assertEqual(pf.get_last_partition(table_name), '2022-01-02')
        self.assertEqual(pf.get_partition_cols(table_name), ['pt'])
        self.assertEqual(pf.get_partition_col(table_name), 'pt')
        self.assertTrue(pf.has_partition_col(table_name))

        f = ColumnFuncs(backend)
        self.assertEqual(f.all_cols_with_exclusion_expr(table_name, 'pt'), 'func_test.id, func_test.val')
        self.assertEqual(f.all_cols_without_one_expr(table_name, 'pt'), 'func_test.id, func_test.val')

        f = TableFuncs(backend)

        self.assertFalse(f.ensure_no_null_data_in_table(step, table_name))
        self.assertTrue(f.ensure_no_null_data_in_table(step, table_name, "id='1'"))
        self.assertTrue(f.check_not_null_column_in_table(step, table_name, 'id'))
        self.assertFalse(f.check_not_null_column_in_table(step, table_name, 'val'))
        self.assertTrue(f.check_not_null_column_in_table(step, table_name, 'val', "id='1'"))

        class _Alerter(Alerter):

            def __init__(self):
                self.alert_msg = None

            def send_alert(self, msg: str, mentioned_users: str = ''):
                self.alert_msg = msg

        alerter = _Alerter()
        f = AlertFunc(backend, alerter)
        f.alert(step, ProcessorContext(VarsContext({'a': 1}), TemplatesContext()), 'test', 'bool({a})', 'result: {a}', 'a,b,c')
        self.assertTrue(alerter.alert_msg is not None)
