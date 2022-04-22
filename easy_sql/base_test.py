import uuid
from datetime import datetime
from typing import Dict, List

from pyspark.sql import SparkSession

from easy_sql.sql_processor import SqlProcessor
from .local_spark import LocalSpark


def should_run_integration_test(key: str = None):
    if key is None or key in ['pg', 'ch', 'mc', 'bq']:
        return False
    return False


dt = lambda dt_s: datetime.strptime(dt_s, '%Y-%m-%d %H:%M:%S')

date = lambda s: datetime.strptime(s, '%Y-%m-%d').date()


def dt_zone(dt_s: str, formate='%Y-%m-%d %H:%M:%S', timezone=None):
    if timezone is None:
        return datetime.strptime(dt_s, formate)
    else:
        return datetime.strptime(dt_s, formate).replace(tzinfo=timezone)


next_id = lambda: str(uuid.uuid1()).replace('-', '')


def run_sql(sql: str, result_table: str,
            funcs: dict = None, variables: dict = None, dry_run: bool = False,
            spark: SparkSession = None, spark_conf: Dict = None) -> List:
    spark = spark or LocalSpark.get(spark_conf)
    processor = SqlProcessor(spark, sql, [], variables or {})
    processor.func_runner.register_funcs(funcs or {})
    processor.run(dry_run=dry_run)
    return spark.sql(f'select * from {result_table}').collect()
