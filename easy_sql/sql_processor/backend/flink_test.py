import unittest

from easy_sql.sql_processor.backend.flink import FlinkTablesConfig

tc = FlinkTablesConfig(
    {
        "databases": [
            {
                "name": "a",
                "tables": [
                    {"name": "a", "schema": ["a int", "b\tvarchar", "`c` char", "'d' int"]},
                    {"name": "b"},
                ],
            }
        ]
    }
)


class FlinkTablesConfigTest(unittest.TestCase):
    def test_table_fields(self):
        self.assertEquals(tc.table_fields("a.a", ["b"]), ["a", "c", "d"])
        self.assertRaises(Exception, lambda: tc.table_fields("b"))
        self.assertRaises(Exception, lambda: tc.table_fields("a.c"))
        self.assertRaises(Exception, lambda: tc.table_fields("a.b"))

    def test_table_options(self):
        self.assertEquals(tc.table_options({"options": {"a": 1}}, {"name": "a"}), {"a": 1})
        self.assertEquals(
            tc.table_options({"options": {"a": 1}}, {"name": "a", "connector": {"options": {"b": 1}}}), {"a": 1, "b": 1}
        )
        self.assertEquals(
            tc.table_options({"options": {"path": "/a/"}}, {"name": "b"}),
            {"path": "/a/b"},
        )
        self.assertEquals(
            tc.table_options({"options": {"path": "/a"}}, {"name": "b"}),
            {"path": "/a/b"},
        )
        self.assertEquals(
            tc.table_options({"options": {"path": "/a/"}}, {"name": "a", "connector": {"options": {"path": "c"}}}),
            {"path": "c"},
        )
