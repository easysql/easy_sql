from typing import Any

from ..logger import logger


def _exec_sql(spark: 'pyspark.sql.SparkSession', sql: str) -> 'pyspark.sql.DataFrame':
    logger.info(f'will exec sql: {sql}')
    return spark.sql(sql)


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
