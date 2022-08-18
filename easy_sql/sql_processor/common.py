from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..logger import logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def _exec_sql(spark: SparkSession, sql: str) -> DataFrame:
    logger.info(f"will exec sql: {sql}")
    return spark.sql(sql)


def is_int_type(type_name):
    return any([type_name.startswith(t) for t in ["integer", "long", "decimal", "short"]])


class Column:
    def __init__(self, name: str, value: Any):
        self.name, self.value = name, value


class SqlProcessorException(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class SqlProcessorAssertionError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class VarsReplacer:
    def replace_variables(self, text: str, include_funcs: bool = True) -> str:
        raise NotImplementedError()
