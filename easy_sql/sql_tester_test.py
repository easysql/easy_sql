import os.path
import unittest
from datetime import datetime

from easy_sql.sql_tester import SqlReader, TableColumnTypes, TestDataFile, work_path


class TableColumnTypesTest(unittest.TestCase):
    def test_col_type(self):
        tt = TableColumnTypes(
            {
                "a": {
                    "id": "int",
                    "val": "string",
                    "val_decimal": "decimal(10, 2)",
                    "val_bool": "boolean",
                    "val_map": "map<string, string>",
                    "val_date": "date",
                    "val_unknown": "unknown",
                    "val_complicated": "struct<latest_value:string,first_show_time:timestamp>",
                    "val_list": "array<string>",
                    "val_list_int": "array<int>",
                }
            },
            {"pt": "string", "data_date": "string"},
            "spark",
        )
        self.assertEqual(tt.get_col_type("a", "id"), "int")
        self.assertRaises(Exception, lambda: tt.get_col_type("_not_exist_table", "_"))
        self.assertEqual(tt.get_col_type("a", "pt"), "string")
        self.assertRaises(Exception, lambda: tt.get_col_type("a", "_not_exist_col"))

        self.assertEqual(tt.cast_as_type("a", "id", "1"), ("int", 1))
        self.assertEqual(tt.cast_as_type("a", "id", None), ("int", None))
        self.assertEqual(tt.cast_as_type("a", "val", "1"), ("string", "1"))
        self.assertEqual(tt.cast_as_type("a", "val", "null"), ("string", None))
        self.assertEqual(tt.cast_as_type("a", "pt", "null"), ("string", None))
        self.assertEqual(tt.cast_as_type("a", "data_date", "2021-01-01"), ("string", "2021-01-01"))
        self.assertEqual(tt.cast_as_type("a", "pt", "2021-01-01"), ("string", "2021-01-01"))
        self.assertEqual(tt.cast_as_type("a", "pt", "20210101.0"), ("string", "20210101"))
        self.assertEqual(tt.cast_as_type("a", "val_decimal", "20210101.0"), ("decimal(10, 2)", 20210101.0))
        self.assertEqual(tt.cast_as_type("a", "val_bool", "true"), ("boolean", True))
        self.assertEqual(tt.cast_as_type("a", "val_bool", "False"), ("boolean", False))
        self.assertEqual(tt.cast_as_type("a", "val_bool", "1"), ("boolean", True))
        self.assertEqual(tt.cast_as_type("a", "val_bool", "0"), ("boolean", True))
        self.assertEqual(tt.cast_as_type("a", "val_bool", ""), ("boolean", False))
        self.assertEqual(tt.cast_as_type("a", "val_list", "a|b|c"), ("array<string>", ["a", "b", "c"]))
        self.assertEqual(tt.cast_as_type("a", "val_list_int", "1|2"), ("array<int>", [1, 2]))
        self.assertEqual(tt.cast_as_type("a", "val_date", "2021-01-01"), ("date", "2021-01-01"))
        self.assertEqual(
            tt.cast_as_type("a", "val_complicated", "a|2021-01-01"),
            ("struct<latest_value:string,first_show_time:timestamp>", ("a", "2021-01-01")),
        )
        self.assertRaises(Exception, lambda: tt.cast_as_type("a", "val_map", "some map"))
        self.assertRaises(Exception, lambda: tt.cast_as_type("a", "val_unknown", "something"))

        tt = TableColumnTypes(
            {"a": {"id": "Int32", "val": "String", "val_list": "Array<String>", "unsupported": "Nested(...)"}},
            {"pt": "String"},
            "clickhouse",
        )
        self.assertEqual(tt.get_col_type("a", "pt"), "String")
        self.assertRaises(Exception, lambda: tt.cast_as_type("a", "unsupported", "null"))


class TestCaseParserTest(unittest.TestCase):
    def excel_parse_case(self, file):
        class _SqlReader(SqlReader):
            def find_file_path(self, file_name: str) -> str:
                return file_name

        work_path.set_work_path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        tdf = TestDataFile(file, _SqlReader())
        cases = tdf.parse_test_cases(TableColumnTypes({}, {}, "spark"))
        self.assertEqual(len(cases), 1)
        case = cases[0]
        self.assertEqual(set(case.includes.keys()), {"some_etl_snippets", "some_etl_snippets1"})
        self.assertEqual(
            case.vars, {"data_date": "2021-01-01", "some_var": 1.0, "some_string_var": 2.0, "some_int_var": 3.0}
        )
        self.assertEqual(case.udf_file_paths, ["test/sample_data_process.py", "not_exist.py"])
        self.assertEqual(case.func_file_paths, ["test/sample_data_process.py"])
        self.assertEqual(len(case.inputs), 1)
        input = case.inputs[0]
        self.assertEqual(input.columns, ["id", "val", "val_date", "data_date"])
        self.assertEqual(input.column_types, ["int", "string", "date", "date"])
        return case

    def test_parse_case(self):
        case = self.excel_parse_case("test/sample_etl.syntax.xlsx")
        input = case.inputs[0]
        self.assertEqual(input.values, [[1, "1.0", datetime(2021, 1, 1, 0, 0), datetime(2021, 1, 1, 0, 0)]])
        self.assertEqual(
            case.outputs[0].values, [[1, "1.0", datetime(2021, 1, 1, 0, 0)], [1, "2.0", None], [1, "3.0", None]]
        )

    def test_int_date_case_from_excel_cell(self):
        case = self.excel_parse_case("test/sample_etl_wps.syntax.xlsx")
        input = case.inputs[0]
        self.assertEqual(input.values, [[1, "1", datetime(2021, 1, 1, 0, 0), datetime(2021, 1, 1, 0, 0)]])
        self.assertEqual(case.outputs[0].values, [[1, "1", datetime(2021, 1, 1, 0, 0)], [1, "2", None], [1, "3", None]])
