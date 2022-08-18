import unittest

from easy_sql.report import EsService, Reporter


class ReporterTest(unittest.TestCase):
    @unittest.skip("integration test")
    def test_should_report_task_result(self):
        reporter = Reporter(EsService("http://testes:9200"))
        reporter.report_task_result("some-task", "some message\nsome other message")
