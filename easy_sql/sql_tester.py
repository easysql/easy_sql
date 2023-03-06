from __future__ import annotations

import datetime as dt
import json
import os
import re
import sys
import unittest
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Union

import openpyxl
from bson import json_util

__all__ = ["SqlTester", "TableData", "TestCase", "WorkPath", "work_path", "SqlReader", "TableColumnTypes"]

from easy_sql.logger import logger
from easy_sql.sql_processor import SqlProcessor
from easy_sql.sql_processor.backend import Backend, Partition, Row, SparkBackend
from easy_sql.sql_processor.backend.rdb import Col, RdbBackend

if TYPE_CHECKING:
    from openpyxl.cell import Cell
    from openpyxl.workbook import Workbook
    from openpyxl.worksheet.worksheet import Worksheet
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

debug_log_enable = True


def log_debug(msg: str):
    if debug_log_enable:
        logger.debug(msg)


class TableData:
    def __init__(
        self,
        name: str,
        columns: List[str],
        column_types: List[str],
        values: List[List[Any]],
        value_descriptions: List[str],
    ):
        self.name, self.columns, self.column_types, self.values, self.value_descriptions = (
            name,
            columns,
            column_types,
            values,
            value_descriptions,
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "columns": json.dumps(self.columns),
            "column_types": json.dumps(self.column_types),
            "value_descriptions": self.value_descriptions,
            "values": [json.dumps(v, default=json_util.default, ensure_ascii=False) for v in self.values],
        }

    def pt_col(self, possible_pt_cols: List[str]) -> Optional[str]:
        for col in possible_pt_cols:
            if col in self.columns:
                return col
        return None

    @staticmethod
    def from_dict(data: Dict) -> TableData:
        return TableData(
            data["name"],
            json.loads(data["columns"]),
            json.loads(data["column_types"]),
            [json.loads(v, object_hook=json_util.object_hook) for v in data["values"]],
            data["value_descriptions"],
        )


class lazy_property:
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, cls):
        val = self.func(instance)
        setattr(instance, self.func.__name__, val)
        return val


class WorkPath:
    def __init__(self, work_path: Optional[str] = None):
        self._work_path: Optional[str] = work_path

    def set_work_path(self, work_path: str):
        self._work_path = work_path

    def work_path(self):
        if not self._work_path:
            raise AssertionError("work_path must be set before use.")
        return self._work_path

    def path(self, relative_path: str):
        return os.path.join(self.work_path(), relative_path)

    def relative_path(self, abs_path: str):
        if not abs_path.startswith(self.work_path()):
            raise AssertionError(f"abs path `{abs_path}` not in work directory: ")
        return "" if len(abs_path) == self.work_path() else abs_path[len(self.work_path()) + 1 :]


work_path = WorkPath()


class TableColumnTypes:
    def __init__(
        self, predefined_table_col_types: Dict[str, Dict[str, str]], partition_col_types: Dict[str, str], backend: str
    ):
        self.table_col_types = predefined_table_col_types
        assert set(partition_col_types.values()).issubset(
            {"int", "string", "Int32", "String"}
        ), f"only int/string partition column types supported, found {partition_col_types}"
        self.partition_col_types = partition_col_types
        self.backend = backend

    def get_col_type(self, table_name: str, col_name: str) -> str:
        col_name = col_name.lower()
        if table_name not in self.table_col_types:
            raise Exception(f"table definition not found from any table files: {table_name}")
        if col_name not in self.table_col_types[table_name]:
            if col_name in self.partition_col_types:
                return self.partition_col_types[col_name]
            raise Exception(
                f"column {col_name} not found for table {table_name}, known columns::"
                f" {list(self.table_col_types[table_name].keys())}"
            )
        if self.backend == "clickhouse":
            return self.table_col_types[table_name][col_name].strip()
        return self.table_col_types[table_name][col_name].lower().strip()

    def column_types_to_schema(
        self, backend: Backend, columns: List[str], types: List[str]
    ) -> Union[StructType, List[Col]]:
        if isinstance(backend, SparkBackend):
            return self.column_types_to_schema_spark(backend.spark, columns, types)
        elif isinstance(backend, RdbBackend):
            return self.column_types_to_schema_rdb(backend, columns, types)
        else:
            raise AssertionError(f"unsupported backend type: {type(backend)}")

    def column_types_to_schema_rdb(self, backend: RdbBackend, columns: List[str], types: List[str]) -> List[Col]:
        return [Col(col, type_) for col, type_ in zip(columns, types)]

    def column_types_to_schema_spark(self, spark: SparkSession, columns: List[str], types: List[str]) -> StructType:
        from pyspark.sql.functions import expr
        from pyspark.sql.types import (
            ArrayType,
            BooleanType,
            DateType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            ShortType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        fields_creators = {
            "int": lambda name: StructField(name, IntegerType()),
            "tinyint": lambda name: StructField(name, ShortType()),
            "bigint": lambda name: StructField(name, LongType()),
            "double": lambda name: StructField(name, DoubleType()),
            "float": lambda name: StructField(name, FloatType()),
            "string": lambda name: StructField(name, StringType()),
            "decimal": lambda name: StructField(name, DoubleType()),
            "boolean": lambda name: StructField(name, BooleanType()),
            "date": lambda name: StructField(name, DateType()),
            "timestamp": lambda name: StructField(name, TimestampType()),
            "array<string>": lambda name: StructField(name, ArrayType(StringType())),  # type: ignore
            "array<int>": lambda name: StructField(name, ArrayType(IntegerType())),  # type: ignore
            "array<tinyint>": lambda name: StructField(name, ArrayType(ShortType())),  # type: ignore
            "array<bigint>": lambda name: StructField(name, ArrayType(LongType())),  # type: ignore
            "array<double>": lambda name: StructField(name, ArrayType(DoubleType())),  # type: ignore
            "array<float>": lambda name: StructField(name, ArrayType(FloatType())),  # type: ignore
            "array<boolean>": lambda name: StructField(name, ArrayType(BooleanType())),  # type: ignore
            "array<date>": lambda name: StructField(name, ArrayType(DateType())),  # type: ignore
            "array<timestamp>": lambda name: StructField(name, ArrayType(TimestampType())),  # type: ignore
        }

        fields = []
        for col, type_ in zip(columns, types):
            type_ = "decimal" if type_.startswith("decimal(") else type_
            if type_ in fields_creators:
                fields.append(fields_creators[type_](col))
            else:
                logger.info(f"try to create null value for `{type_}` when convert to spark schema for column `{col}`")
                field_schema = (
                    spark.createDataFrame(spark.sparkContext.emptyRDD(), StructType([]))
                    .withColumn(col, expr(f"cast(null as {type_})"))
                    .schema.fields[0]
                )
                fields.append(field_schema)

        return StructType(fields)

    def cast_as_type(
        self,
        table_name: str,
        col_name: str,
        col_value: Any,
        date_converter: Optional[Callable] = None,
        col_type: Optional[str] = None,
    ) -> Tuple[str, Any]:
        col_type = self.get_col_type(table_name, col_name) if col_type is None else col_type
        col_type = col_type.strip()
        if self.backend == "clickhouse":
            if "Nested" in col_type:
                raise AssertionError("clickhouse backend can not support Nested col type")
            # for these type in clickhouse is case insensitive
            if col_type.lower() in ["bool", "date", "datetime", "decimal"]:
                col_type = col_type.lower()
        else:
            col_type = col_type.lower()
        if col_value is None or (isinstance(col_value, str) and col_value.strip() == "null"):
            return col_type, None

        def process_pt_val(date_converter: Optional[Callable], col_value: str):
            if not date_converter:
                return str(col_value).strip()
            return date_converter(col_value).strftime("%Y-%m-%d") if date_converter(col_value) else None

        try:
            if col_name in self.partition_col_types:
                backend_is_bigquery = self.backend == "bigquery"
                if backend_is_bigquery or col_name == "data_date":
                    # for bigquery, partition column must be a date
                    return col_type, process_pt_val(date_converter, col_value)
                else:
                    try:
                        # try convert partition column to int to remove possible float values
                        return col_type, str(round(float(col_value))) if col_type == "string" else round(
                            float(col_value)
                        )
                    except ValueError as e:
                        if col_type == "string":
                            return col_type, str(col_value)
                        else:
                            raise e

            if col_type.replace(" ", "").startswith("map<"):
                raise AssertionError(
                    f"map type not supported right now when parsing value `{col_value}` for column:"
                    f" {table_name}.{col_name}"
                )
            if col_type.startswith("decimal(") or col_type == "double" or col_type == "float":
                return col_type, float(col_value)
            if col_type in ["bigint", "int", "tinyint", "Int32", "Int64", "UInt32", "UInt64"]:
                return col_type, int(col_value)
            if col_type in ["boolean", "bool"]:
                if str(col_value).lower() == "true":
                    return col_type, True
                elif str(col_value).lower() == "false":
                    return col_type, False
                return col_type, bool(col_value)
            if col_type in ["string", "text", "String"]:
                return col_type, str(col_value).strip()
            if col_type.replace(" ", "") in ["array<string>", "text[]", "Array(String)"]:
                return col_type, [s.strip() for s in str(col_value).strip().split("|") if s.strip()]
            if col_type.replace(" ", "") in ["array<int>", "int[]", "Array(Int)"]:
                return col_type, [int(s.strip()) for s in str(col_value).strip().split("|") if s.strip()]
            if col_type.replace(" ", "") == "struct<latest_value:string,first_show_time:timestamp>":
                vals = str(col_value).strip().split("|")
                if len(vals) < 2:
                    raise AssertionError(
                        f"must provide all the values of type {col_type} for {table_name}.{col_name}. Incomplete value"
                        f" `{col_value}`"
                    )
                latest_value = str(vals[0]).strip()
                first_show_time = str(vals[1]).strip() if not date_converter else date_converter(vals[1])
                return col_type, (latest_value, first_show_time)
            if col_type in ["date", "timestamp", "datetime"]:
                if not date_converter:
                    return col_type, str(col_value).strip()
                else:
                    return col_type, date_converter(col_value)
        except Exception as e:
            raise Exception(f"convert {col_type} failed for {table_name}.{col_name} for value `{col_value}`", e)

        raise AssertionError(
            f"not supported type {col_type} for column {table_name}.{col_name} when parsing value {col_value}"
        )


class TestCase:
    def __init__(
        self, sql_file_path: Optional[str] = None, sql_file_content: Optional[str] = None, default_col_type="string"
    ):
        self.name, self.vars = None, {}
        self.includes = {}
        self.inputs: List[TableData] = []
        self.outputs: List[TableData] = []
        assert sql_file_path is not None or sql_file_content is not None, "sql_file_path or sql_file_content required"
        self.sql_file_path = sql_file_path
        self.sql_file_content = sql_file_content
        self.udf_file_paths: List[str] = []
        self.func_file_paths: List[str] = []
        self.default_col_type = default_col_type

    def as_dict(self):
        data = {
            attr: getattr(self, attr)
            for attr in dir(self)
            if not attr.startswith("_") and not callable(getattr(self, attr))
        }
        data.update(
            {
                "inputs": [table_data.as_dict() for table_data in self.inputs],
                "outputs": [table_data.as_dict() for table_data in self.outputs],
            }
        )
        return data

    @staticmethod
    def from_dict(data: Dict) -> TestCase:
        case = TestCase(data["sql_file_path"], data.get("sql_file_content", None))
        case.name = data["name"]
        case.vars = data["vars"]
        case.includes = data["includes"] if "includes" in data else {}
        case.inputs = [TableData.from_dict(table_data_dict) for table_data_dict in data["inputs"]]
        case.outputs = [TableData.from_dict(table_data_dict) for table_data_dict in data["outputs"]]
        case.udf_file_paths = data["udf_file_paths"] if "udf_file_paths" in data else []
        case.func_file_paths = data["func_file_paths"] if "func_file_paths" in data else []
        return case

    @property
    def simple_sql_name(self):
        return os.path.basename(self.sql_file_path) if self.sql_file_path else None

    def parse_test_case_of_label(
        self, wb: Workbook, label: str, row_start_idx: int, rows: List[List[Cell]], table_column_types: TableColumnTypes
    ):
        log_debug(f"start to parse case from row {row_start_idx} for label {label}")
        if label == "CASE":
            self.name = str(rows[0][1].value).strip()
        elif label == "VARS":
            self.parse_vars(wb, row_start_idx, rows)
        elif label == "INCLUDES":
            self.parse_includes(wb, row_start_idx, rows)
        elif label == "INPUT":
            self.parse_input(wb, row_start_idx, rows, table_column_types)
        elif label == "OUTPUT":
            self.parse_output(wb, row_start_idx, rows, table_column_types)
        elif label == "UDFS":
            self.parse_udfs(row_start_idx, rows)
        elif label == "FUNCS":
            self.parse_funcs(row_start_idx, rows)
        else:
            raise AssertionError(f"unknown label: {label}")

    def parse_includes(self, wb: Workbook, row_start_idx: int, rows: List[List[Cell]]):
        for i, row in enumerate(rows):
            include_name, include_value = row[1].value and row[1].value.strip(), row[2].value and row[2].value.strip()  # type: ignore
            if include_name:
                if not include_value:
                    raise AssertionError(
                        f"parse test case at row {row_start_idx + 1 + i} failed: there must be value set for INCLUDES,"
                        " found None"
                    )
                self.includes[include_name] = include_value
                log_debug(
                    f"find includes at row {row_start_idx + 1}: include_name={include_name},"
                    f" include_value={include_value}"
                )

    def parse_udfs(self, row_start_idx: int, rows: List[List[Cell]]):
        for udf_path in rows[0][1:]:
            if udf_path.value and udf_path.value.strip():  # type: ignore
                self.udf_file_paths.append(udf_path.value)  # type: ignore
                log_debug(f"find udf_path at row {row_start_idx + 1}: udf_relative_path={udf_path.value}")

    def parse_funcs(self, row_start_idx: int, rows: List[List[Cell]]):
        for func_path in rows[0][1:]:
            if func_path.value and func_path.value.strip():  # type: ignore
                self.func_file_paths.append(func_path.value)  # type: ignore
                log_debug(f"find func_path at row {row_start_idx + 1}: func_relative_path={func_path.value}")

    def parse_vars(self, wb: Workbook, row_start_idx: int, rows: List[List[Cell]]):
        if len(rows) < 2:
            raise AssertionError(
                f"parse test case at row {row_start_idx + 1} failed: there must be value set for VARS, found None"
            )
        else:
            for var_name, var_value in zip(rows[0][1:], rows[1][1:]):
                if var_name.value and var_name.value.strip():  # type: ignore
                    var_name, var_value = self.parse_var_from_cell(wb, var_name, var_value)
                    self.vars[var_name] = var_value
                    log_debug(
                        f"find vars at row {row_start_idx + 1}: name={var_name}, value={var_value} (type:"
                        f" {type(var_value)})"
                    )

    def parse_var_from_cell(self, wb: Workbook, var_name: Cell, var_value: Cell) -> Tuple[str, Any]:
        name = var_name.value.strip()  # type: ignore
        if name.lower() == "data_date":
            value = self.parse_cell_value_as_date(wb, var_value.value)
            value = value.strftime("%Y-%m-%d") if value else None
        else:
            value = var_value.value
        return name, value

    def parse_cell_value_as_date(self, wb: Workbook, value: Any) -> Optional[datetime]:
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return None
        elif isinstance(value, str) and value.strip() != "":
            value = value.strip()
            if len(value) != len("2000-01-01") and len(value) != len("2000-01-01 00:00:00"):
                raise AssertionError("date column must be of format `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss`")
            format = "%Y-%m-%d" if len(value) == len("2000-01-01") else "%Y-%m-%d %H:%M:%S"
            return datetime.strptime(value, format)
        # Excel for Windows stores dates by default as the number of days (or fraction thereof) since 1899-12-31T
        # https://stackoverflow.com/questions/3727916/how-to-use-xlrd-xldate-as-tuple
        elif isinstance(value, int):
            delta = dt.timedelta(days=value)
            return dt.datetime.strptime("1899-12-30", "%Y-%m-%d") + delta
        elif isinstance(value, datetime):
            return value
        else:
            raise Exception(f"unknown date cell value: {value}")

    def parse_output(
        self, wb: Workbook, row_start_idx: int, rows: List[List[Cell]], table_column_types: TableColumnTypes
    ):
        self.outputs.append(self.parse_table(wb, "OUTPUT", row_start_idx, rows, table_column_types))

    def parse_input(
        self, wb: Workbook, row_start_idx: int, rows: List[List[Cell]], table_column_types: TableColumnTypes
    ):
        self.inputs.append(self.parse_table(wb, "INPUT", row_start_idx, rows, table_column_types))

    def parse_table(
        self, wb: Workbook, label: str, row_start_idx: int, rows: List[List[Cell]], table_column_types: TableColumnTypes
    ) -> TableData:
        cells = rows[0]
        if len(cells) < 2 or not cells[1].value or not str(cells[1].value).strip():
            raise AssertionError(
                f"parse test case at row {row_start_idx + 1} failed: there must be table name set for {label}, found"
                " None"
            )
        if len(cells) < 3 or not cells[2].value or not str(cells[2].value).strip():
            raise AssertionError(
                f"parse test case at row {row_start_idx + 1} failed: there must be at least one column set for {label},"
                " found 0"
            )

        table_name = str(cells[1].value)

        columns, column_types = [], []
        type_in_column_names = self.is_type_in_column_names(cells)

        for cell in cells[2:]:
            if not cell.value or not str(cell.value).strip():
                break
            column_name = str(cell.value).strip()
            if type_in_column_names:
                if ":" in column_name:
                    columns.append(column_name[: column_name.index(":")])
                    column_types.append(column_name[column_name.index(":") + 1 :])
                else:
                    columns.append(column_name)
                    column_types.append(self.default_col_type)  # if no type specified, use string as default
            else:
                columns.append(column_name)
                if "." in table_name:
                    column_types.append(table_column_types.get_col_type(table_name, columns[-1]))
                else:
                    raise Exception(f"Unable to resolve types for table: {table_name}")

        values, value_descriptions = [], []
        for row_idx, cells in enumerate(rows[1:]):
            value_cells = cells[2:]
            has_values = any([value_cells[i].value not in [None, ""] for i in range(len(columns))])
            if cells[1].value and str(cells[1].value).strip():
                value_descriptions.append(str(cells[1].value).strip())
                values.append(
                    self._parse_table_row_values(
                        wb, table_name, columns, row_idx, row_start_idx, value_cells, column_types, table_column_types
                    )
                )
            elif label == "INPUT":
                # If no description mentioned for this row, just ignore it.
                # This ensures we must add description for a row, to clarify how the data comes.
                if has_values:
                    raise AssertionError(
                        f"no description for table({table_name}) data at row {row_start_idx + 1 + row_idx + 1}"
                    )
            elif label == "OUTPUT":
                has_values = any([value_cells[i].value not in [None, ""] for i in range(len(columns))])
                if has_values:
                    values.append(
                        self._parse_table_row_values(
                            wb,
                            table_name,
                            columns,
                            row_idx,
                            row_start_idx,
                            value_cells,
                            column_types,
                            table_column_types,
                        )
                    )

        log_debug(
            f'find {label.lower()} table at row {row_start_idx + 1}: {table_name}{{{", ".join(columns)}}}, data length'
            f" is {len(values)}"
        )
        print(f"table data for `{table_name}`: ")
        for v in values:
            print(v)
        return TableData(table_name, columns, column_types, values, value_descriptions)

    def is_type_in_column_names(self, cells) -> bool:
        for cell in cells[2:]:
            if not cell.value or not str(cell.value).strip():
                break
            column_name = str(cell.value).strip()
            if ":" in column_name:
                return True
        return False

    def _parse_table_row_values(
        self,
        wb: Workbook,
        table_name: str,
        columns: List[str],
        row_idx: int,
        row_start_idx: int,
        value_cells: List[Cell],
        column_types: List[str],
        table_column_types: TableColumnTypes,
    ):
        row_values = []
        for col_idx in range(len(columns)):
            try:
                _, value = table_column_types.cast_as_type(
                    table_name,
                    columns[col_idx],
                    value_cells[col_idx].value,
                    date_converter=lambda v: self.parse_cell_value_as_date(wb, v),
                    col_type=column_types[col_idx] if len(column_types) > 0 else None,
                )
                row_values.append(value)
            except Exception as e:
                if e.args[0].startswith("convert "):
                    raise Exception(
                        f"when parsing values at row: {row_start_idx + 1 + row_idx + 1}, col: {col_idx + 2 + 1}", e
                    )
                else:
                    raise e
        return row_values

    def read_sql_content(self) -> str:
        if self.sql_file_content:
            return self.sql_file_content
        else:
            if self.sql_file_path is None:
                raise AssertionError("can not find the sql file having same name with test file")
            with open(work_path.path(self.sql_file_path), "r") as f:
                return f.read()

    @property
    def completed(self):
        return self.name and len(self.inputs) and len(self.outputs)

    @property
    def missed_fields(self):
        fields = []
        if not self.name:
            fields.append("name")
        if not len(self.inputs):
            fields.append("inputs")
        if not len(self.outputs):
            fields.append("outputs")
        return fields


class TestDataFile:
    def __init__(self, test_data_file: str, sql_reader: SqlReader, backend: str = "spark"):
        self.test_data_file = test_data_file
        self.sql_reader = sql_reader
        self.wb = openpyxl.load_workbook(self.test_data_file, data_only=True)
        self.backend = backend

    def parse_test_cases(self, table_column_types: TableColumnTypes) -> List[TestCase]:
        wb = self.wb
        test_cases = []
        for sheet_name in wb.sheetnames:
            if not sheet_name.lower().startswith("suit"):
                continue
            for case in self.parse_test_cases_from_sheet(wb[sheet_name], table_column_types):  # type: ignore
                test_cases.append(case)
        return test_cases

    def parse_test_cases_from_sheet(self, sheet: Worksheet, table_column_types: TableColumnTypes) -> List[TestCase]:
        cases = []
        last_row_idx = -1
        cases_rows = []
        cases_start_indices = []
        for row_idx, row in enumerate(sheet.rows):
            cells = list(row)
            if cells[0].value and cells[0].value.strip() == "CASE":
                if last_row_idx == -1:
                    last_row_idx = row_idx
                log_debug(f"add case at: {row_idx}")
                cases_start_indices.append(row_idx)
                cases_rows.append([cells])
            elif last_row_idx != -1:
                cases_rows[-1].append(cells)
        for case_start_idx, case_rows in zip(cases_start_indices, cases_rows):
            cases.append(self.parse_test_case(case_start_idx, case_rows, table_column_types))
        return cases

    def parse_test_case(
        self, case_start_idx: int, case_rows: List[List[Cell]], table_column_types: TableColumnTypes
    ) -> TestCase:
        if self.backend == "clickhouse":
            default_col_type = "String"
        else:
            default_col_type = "string"
        if self.sql_reader.read_as_content(self.test_data_file):
            case = TestCase(
                sql_file_content=self.sql_reader.read_sql(self.test_data_file), default_col_type=default_col_type
            )
        else:
            case = TestCase(
                self.sql_reader.find_file_path(self.test_data_file[: self.test_data_file.rindex(".")] + ".sql")
            )
        last_label, last_label_idx = None, -1
        for i, row in enumerate(case_rows):
            cells = row
            label: str = cells[0].value and cells[0].value.strip()  # type: ignore
            if label in ["CASE", "VARS", "INCLUDES", "INPUT", "OUTPUT", "UDFS", "FUNCS"]:
                if last_label is not None:
                    case.parse_test_case_of_label(
                        self.wb,
                        last_label,
                        case_start_idx + last_label_idx,
                        case_rows[last_label_idx:i],
                        table_column_types,
                    )
                last_label, last_label_idx = label, i
        if last_label:
            case.parse_test_case_of_label(
                self.wb, last_label, case_start_idx + last_label_idx, case_rows[last_label_idx:], table_column_types
            )

        if not case.completed:
            raise AssertionError(f"parse test case failed, got incomplete case, missed fields: {case.missed_fields}")

        return case


class TestResult:
    PASSED = "PASSED"
    FAILED = "FAILED"

    def __init__(self, test_data_file: str):
        self.test_data_file = test_data_file
        self.case_results = []
        self.cases: List[TestCase] = []

    def collect_case_result(self, case: TestCase, result: str):
        self.cases.append(case)
        self.case_results.append({"case_name": case.name, "result": result})

    @property
    def failed_cases(self):
        return list(filter(lambda cr: cr["result"] == TestResult.FAILED, self.case_results))

    @property
    def is_fail(self) -> bool:
        return any([r["result"] == TestResult.FAILED for r in self.case_results])

    @property
    def is_success(self) -> bool:
        return not self.is_fail

    @property
    def passed_cases(self):
        return list(filter(lambda cr: cr["result"] == TestResult.PASSED, self.case_results))

    @property
    def simple_stat_str(self) -> str:
        return f"{len(self.passed_cases)} PASSED, {len(self.failed_cases)} FAILED"

    def print_result(self):
        print(f"test result for {self.test_data_file}: {self.simple_stat_str}")
        if len(self.failed_cases):
            print(f"failed cases: {', '.join([cr['case_name'] for cr in self.failed_cases])}")

    @staticmethod
    def print_results(test_results: List[TestResult]):
        print("======================Test Report====================")
        for tr in test_results:
            tr.print_result()
        failed_count = sum([len(tr.failed_cases) for tr in test_results])
        passed_count = sum([len(tr.passed_cases) for tr in test_results])
        if failed_count == 0:
            print(f"\nCongratulation! All {passed_count} tests passed!")
        else:
            print(f"\nThere are test failures! {passed_count} passed, {failed_count} failed")


class TestCaseRunner:
    def __init__(
        self,
        env: str,
        dry_run: bool,
        backend_creator: Callable,
        table_column_types: TableColumnTypes,
        unit_test_case: Optional[unittest.TestCase],
        sql_processor_creator: Callable,
    ):
        self.unit_test_case = unit_test_case or unittest.TestCase()
        self.env, self.dry_run, self.backend_creator = env, dry_run, backend_creator
        self.sql_processor_creator = sql_processor_creator
        self.table_column_types = table_column_types
        self.collected_sql = None

    def run_test(self, case: TestCase):
        sql = case.read_sql_content()

        backend = self.backend_creator(case)
        try:
            self.clean(case, backend)
            self.create_inputs(case, backend)

            sql_processor = self.create_sql_processor(backend, case, sql)

            sql_processor.run(self.dry_run)

            self.collected_sql = sql_processor.sql_collector.collected_sql()

            self.verify_outputs(backend, case)
        finally:
            backend.clean()

    def verify_outputs(self, backend: Backend, case: TestCase):
        tempviews = backend.temp_tables()
        print("tempviews after test:", tempviews)
        for output in case.outputs:
            tempview_name = self.find_temp_view_for_output(case, output, tempviews)
            actual_output, expected_output = self.get_data(backend, output, tempview_name)

            def list_item_to_set(values: List[Union[List, Row]]):
                result = []
                for row in values:
                    result.append([set(v) if isinstance(v, list) else v for v in row])
                return result

            print("will verify equality for output: ", output.name)
            print("expected output: ", list_item_to_set(expected_output))  # type: ignore
            print("actual output: ", list_item_to_set(actual_output))  # type: ignore
            self.unit_test_case.assertListEqual(list_item_to_set(expected_output), list_item_to_set(actual_output))  # type: ignore

    def create_sql_processor(self, backend: Backend, case: TestCase, sql: str) -> SqlProcessor:
        sql_processor = self.sql_processor_creator(backend, sql, case)
        for udf_file in case.udf_file_paths:
            sql_processor.register_udfs_from_pyfile(work_path.path(udf_file))
            log_debug(f"registering udf from `{work_path.path(udf_file)}` for case `{case.name}`")
        for func_file in case.func_file_paths:
            sql_processor.register_funcs_from_pyfile(work_path.path(func_file))
            log_debug(f"registering funcs from `{work_path.path(func_file)}` for case `{case.name}`")
        return sql_processor

    def get_data(self, backend: Backend, output: TableData, tempview_name: str) -> Tuple[List[Row], List[Row]]:
        full_tempview_name = f"{backend.temp_schema}.{tempview_name}" if backend.is_bigquery_backend else tempview_name  # type: ignore
        select_output_sql = (
            f'select {", ".join(output.columns)} from {full_tempview_name} order by {", ".join(output.columns)}'
        )
        actual_output = backend.exec_sql(select_output_sql).collect()

        schema = self.table_column_types.column_types_to_schema(backend, output.columns, output.column_types)
        backend.create_temp_table_with_data(f"{full_tempview_name}__expected", output.values, schema)
        select_output_sql = (
            f'select {", ".join(output.columns)} from {full_tempview_name}__expected order by'
            f' {", ".join(output.columns)}'
        )
        expected_output = backend.exec_sql(select_output_sql).collect()
        return actual_output, expected_output

    def find_temp_view_for_output(self, case: TestCase, output: TableData, tempviews: List[str]) -> str:
        # tempview name format: {output_pure_table_name}_{md5(xxx)}
        hex_pattern = r"^[a-f0-9]+_output$" if self.dry_run else r"^[a-f0-9]+$"
        if "." in output.name:
            tempview_name = output.name[output.name.find(".") + 1 :]
            tempview_name = [
                tv
                for tv in tempviews
                if tv.startswith(tempview_name) and re.match(hex_pattern, tv[len(tempview_name) + 1 :])
            ]
        else:
            tempview_name = [tv for tv in tempviews if tv == output.name]
        if len(tempview_name) == 0:
            raise Exception(
                f"output `{output.name}` not found after execute test: {case.simple_sql_name}.{case.name}. "
                f"All temporary views are: {tempviews}"
            )
        elif len(tempview_name) > 1:
            raise Exception(
                f"multiple temp views found for output `{output.name}` found after execute test: {tempview_name}"
            )
        else:
            tempview_name = tempview_name[0]
        return tempview_name

    def create_inputs(self, case: TestCase, backend: Backend):
        for input in case.inputs:
            schema = self.table_column_types.column_types_to_schema(backend, input.columns, input.column_types)
            if "." in input.name:
                print(f"creating table: {input.name}")
                pt_col = input.pt_col(list(self.table_column_types.partition_col_types.keys()))
                backend.create_table_with_data(input.name, input.values, schema, [Partition(pt_col)] if pt_col else [])
            else:
                print(f"creating temp table: {input.name}", input.columns, input.column_types)
                backend.create_temp_table_with_data(input.name, input.values, schema)

    def clean(self, case: TestCase, backend: Backend):
        databases = set()
        table_names = set.union({input.name for input in case.inputs}, {output.name for output in case.outputs})
        for table_name in table_names:
            if "." in table_name:
                databases.add(table_name.split(".")[0])
        for db in databases:
            try:
                if backend.is_bigquery_backend:
                    backend.exec_native_sql(f"drop schema if exists {db} cascade")
                elif backend.is_clickhouse_backend:
                    backend.exec_native_sql(f"drop database if exists {db}")
            except Exception as e:
                # BigQuery will throw an exception when deleting a nonexistent dataset even if using [IF EXISTS]
                import re

                if not re.match(
                    r"[\s\S]*Permission bigquery.datasets.delete denied on dataset[\s\S]*(or it may not exist)[\s\S]*",
                    str(e.args[0]),
                ):
                    raise e


class SqlTester:
    def __init__(
        self,
        backend_creator: Optional[Callable[[TestCase], Backend]] = None,
        table_column_types: Optional[TableColumnTypes] = None,
        sql_reader_creator: Optional[Callable[[], SqlReader]] = None,
        sql_processor_creator: Optional[Callable[[Backend, str, TestCase], SqlProcessor]] = None,
        unit_test_case: Optional[unittest.TestCase] = None,
        dry_run: bool = True,
        env: str = "test",
        work_dir: Optional[str] = None,
        backend: str = "spark",
        scala_udf_initializer: Optional[str] = None,
    ):
        if work_dir is not None:
            work_path.set_work_path(os.path.abspath(work_dir))

        def create_sql_processor(backend, sql, case):
            if backend.is_bigquery_backend:
                vars = dict(case.vars.items() | {"temp_db": backend.temp_schema}.items())
            else:
                vars = case.vars
            return SqlProcessor(
                backend, sql, [], variables=vars, scala_udf_initializer=scala_udf_initializer, includes=case.includes
            )

        self.sql_processor_creator = sql_processor_creator or create_sql_processor
        self.backend = backend
        self.table_column_types = table_column_types or TableColumnTypes({}, {}, backend)
        self.unit_test_case = unit_test_case
        self.dry_run = dry_run
        self.env = env
        self.sql_reader = sql_reader_creator() if sql_reader_creator else SqlReader()
        self.collected_sql = None
        if backend_creator:
            self.test_case_runner = TestCaseRunner(
                self.env,
                self.dry_run,
                backend_creator,
                self.table_column_types,
                sql_processor_creator=self.sql_processor_creator,
                unit_test_case=self.unit_test_case,
            )

    def run_tests(self, test_data_files: List[str]):
        test_results = []
        for f in test_data_files:
            print(f"============= running test from file {os.path.basename(f)} [START] ==============")
            test_result = self.run_test(f)
            test_results.append(test_result)
            print(
                f"============= running test from file {os.path.basename(f)} [END:"
                f" {test_result.simple_stat_str}]=============="
            )
        TestResult.print_results(test_results)
        failed_count = sum([len(test_result.failed_cases) for test_result in test_results])
        if failed_count:
            sys.exit(1)

    def run_test(self, test_data_file: str, case_idx: int = -1) -> TestResult:
        cases = self.parse_test_cases(test_data_file, self.table_column_types)

        if case_idx != -1 and (case_idx < 0 or case_idx >= len(cases)):
            raise AssertionError(f"test case {case_idx} not found. {len(cases)} cases in {test_data_file} are found.")
        cases = cases if case_idx == -1 else [cases[case_idx]]

        tr = TestResult(test_data_file)
        for case in cases:
            passed = self.run_case(case)
            tr.collect_case_result(case, TestResult.PASSED if passed else TestResult.FAILED)  # type: ignore
        return tr

    def parse_test_cases(self, test_data_file, table_column_types: TableColumnTypes) -> List[TestCase]:
        if test_data_file.endswith(".xlsx"):
            cases = TestDataFile(test_data_file, sql_reader=self.sql_reader, backend=self.backend).parse_test_cases(
                table_column_types
            )
        elif test_data_file.endswith(".json"):
            with open(test_data_file, "r") as f:
                cases = json.loads(f.read(), object_hook=json_util.object_hook)
            cases = [TestCase.from_dict(case_dict) for case_dict in cases]
        else:
            raise AssertionError(f"unsupported format of test file: {test_data_file}")
        return cases

    def run_case(self, case: TestCase) -> bool:
        try:
            self.test_case_runner.run_test(case)
            self.collected_sql = self.test_case_runner.collected_sql
            return True
        except Exception:
            import traceback

            traceback.print_exc()
            return False

    def convert_cases_to_json(self, test_data_file: str):
        if not test_data_file.endswith(".xlsx"):
            raise AssertionError(f"only support to convert excel file, got `{test_data_file}`")
        cases = TestDataFile(test_data_file, sql_reader=self.sql_reader).parse_test_cases(self.table_column_types)
        data = [case.as_dict() for case in cases]
        output_file = test_data_file[: -len(".xlsx")] + ".json"
        with open(output_file, "w") as f:
            f.write(json.dumps(data, default=json_util.default, ensure_ascii=False, indent=2, sort_keys=True))
            logger.info(f"created file: {output_file}")

    def generate_python_unittest_file(self, test_data_file: str, backend: str = "spark"):
        cases = self.parse_test_cases(test_data_file, self.table_column_types)
        py_file = os.path.join(
            os.path.dirname(test_data_file),
            os.path.basename(test_data_file).replace(".", "__").replace("__xlsx", "_test.py"),
        )
        import jinja2

        env = jinja2.Environment(
            loader=jinja2.DictLoader(
                {
                    "py_file_tpl": f"""import os
import unittest
import sys
import importlib

try:
    sys.path.insert(0, 'common')
    SqlTester = importlib.import_module('sql_test').SqlTester  # type: ignore
except ModuleNotFoundError as e:
    print('using SqlTester from dataplat.')
    from {self.__module__} import SqlTester


class SqlTest(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        work_dir = os.environ.get('WORK_DIR')
        self.sql_tester = SqlTester(unit_test_case=self, work_dir=work_dir, backend='{backend}')
        this_file = os.path.abspath(__file__)
        self.test_data_file = os.path.join(os.path.dirname(this_file), os.path.basename(this_file).replace('__', '.').replace('_test.py', '.json'))
        if not os.path.isfile(self.test_data_file):
            self.test_data_file = os.path.join(os.path.dirname(this_file), os.path.basename(this_file).replace('__', '.').replace('_test.py', '.xlsx'))
{{% for case in cases %}}
    def test_{{{{loop.index}}}}(self):
        # {{{{ case.name }}}}
        self.assertTrue(self.sql_tester.run_test(self.test_data_file, {{{{loop.index0}}}}).is_success)

{{% endfor %}}
if __name__ == '__main__':
    unittest.main()
"""
                }
            )
        )
        with open(py_file, "wb") as f:
            env.get_template("py_file_tpl").stream(cases=cases).dump(f, encoding="utf8")
            logger.info(f"created file: {py_file}")


class SqlReader:
    def read_sql(self, test_data_file: str) -> str:
        raise NotImplementedError()

    def find_file_path(self, file_name: str) -> str:
        file_name = os.path.basename(file_name)
        for root, _dirs, files in os.walk(work_path.work_path()):
            for file in files:
                if file == file_name:
                    return work_path.relative_path(os.path.join(root, file_name))
        raise Exception("file not found: " + file_name)

    def read_as_content(self, test_data_file: str):
        return False

    def read_as_file_path(self, test_data_file: str):
        return not self.read_as_content(test_data_file)
