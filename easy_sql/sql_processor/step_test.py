import unittest

from easy_sql.sql_processor import StepConfig, SqlProcessorException


class StepConfigTest(unittest.TestCase):

    def test_should_parse_config(self):
        self.assertEqual(StepConfig.from_config_line('-- target=check.f1', 0),
                         StepConfig('check', 'f1', None, 0))
        self.assertEqual(StepConfig.from_config_line('-- target=check.f1(a, ${b})', 0),
                         StepConfig('check', 'f1(a, ${b})', None, 0))
        self.assertEqual(StepConfig.from_config_line('-- target=check.f1(a, ${b}), if=f2(c, ${d})', 0),
                         StepConfig('check', 'f1(a, ${b})', 'f2(c, ${d})', 0))
        self.assertEqual(StepConfig.from_config_line('-- target=check.f1(a, ${b}),if=f2(c, ${d})', 0),
                         StepConfig('check', 'f1(a, ${b})', 'f2(c, ${d})', 0))
        self.assertEqual(StepConfig.from_config_line('-- target=variables', 0),
                         StepConfig('variables', None, None, 0))
        self.assertEqual(StepConfig.from_config_line('-- target=variables, if=f2(c, ${d})', 0),
                         StepConfig('variables', None, 'f2(c, ${d})', 0))
        with self.assertRaises(expected_exception=SqlProcessorException):
            StepConfig.from_config_line('-- target=check.f1(a, ${b}),if=f2-(c, ${d})', 0)
        with self.assertRaises(expected_exception=SqlProcessorException):
            StepConfig.from_config_line('-- target=unknown_type', 0)


if __name__ == '__main__':
    unittest.main()
