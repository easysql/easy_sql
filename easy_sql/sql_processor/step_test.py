import unittest

from easy_sql.sql_processor import SqlProcessorException, StepConfig
from easy_sql.sql_processor.step import SqlCleaner, StepFactory


class StepConfigTest(unittest.TestCase):
    def test_should_parse_config(self):
        self.assertEqual(StepConfig.from_config_line("-- target=check.f1", 0), StepConfig("check", "f1", None, 0))
        self.assertEqual(
            StepConfig.from_config_line("-- target=check.f1(a, ${b})", 0), StepConfig("check", "f1(a, ${b})", None, 0)
        )
        self.assertEqual(
            StepConfig.from_config_line("-- target=check.f1(a, ${b}), if=f2(c, ${d})", 0),
            StepConfig("check", "f1(a, ${b})", "f2(c, ${d})", 0),
        )
        self.assertEqual(
            StepConfig.from_config_line("-- target=check.f1(a, ${b}),if=f2(c, ${d})", 0),
            StepConfig("check", "f1(a, ${b})", "f2(c, ${d})", 0),
        )
        self.assertEqual(StepConfig.from_config_line("-- target=variables", 0), StepConfig("variables", None, None, 0))
        self.assertEqual(
            StepConfig.from_config_line("-- target=variables, if=f2(c, ${d})", 0),
            StepConfig("variables", None, "f2(c, ${d})", 0),
        )
        with self.assertRaises(expected_exception=SqlProcessorException):
            StepConfig.from_config_line("-- target=check.f1(a, ${b}),if=f2-(c, ${d})", 0)
        with self.assertRaises(expected_exception=SqlProcessorException):
            StepConfig.from_config_line("-- target=unknown_type", 0)

    def test_should_clean_sql(self):
        self.assertEquals(
            """
        with a as (select 1 as a) -- comment
        --comment
select * from a
        """.strip(),
            SqlCleaner().clean_sql(
                """
        -- comment
        with a as (select 1 as a) -- comment
        --comment
        select * from a -- comment
        ;
        --comment
        """
            ),
        )

    def test_should_clean_sql_with_semicolon_before_comment(self):
        self.assertEquals(
            """
        with a as (select 1 as a) -- comment
        --comment
select * from a
        """.strip(),
            SqlCleaner().clean_sql(
                """
        -- comment
        with a as (select 1 as a) -- comment
        --comment
        select * from a; -- comment
        ;
        --comment
        """
            ),
        )

    def test_should_read_sql_correctly(self):
        sql = """
-- target=temp.test
select ';' as a
        """
        steps = StepFactory(None, None).create_from_sql(sql, {})  # type: ignore
        self.assertEquals(1, len(steps))
        assert steps[0].target_config is not None
        self.assertEquals(steps[0].target_config.name, "test")
        assert steps[0].select_sql is not None
        self.assertEquals(steps[0].select_sql.strip(), "select ';' as a")

    def test_should_skip_duplicate_include(self):
        sql0 = """
-- target=temp.test
select 1 as a
        """
        sql1 = """
-- include 0 start
-- include=0.sql
        """
        sql = """
-- outer include start
-- include=1.sql
-- include=1.sql
-- include=0.sql
        """
        sql_expected = """
-- outer include start
-- include 0 start
-- target=temp.test
select 1 as a
"""
        sf = StepFactory(None, None, skip_duplicate_include=True)  # type: ignore
        sf.create_from_sql(sql, {"0.sql": sql0, "1.sql": sql1})
        print(sf.resolved_sql)
        assert (
            "\n".join([line.strip() for line in sf.resolved_sql.splitlines() if line.strip()]) == sql_expected.strip()
        )


if __name__ == "__main__":
    unittest.main()
