import os

from pyspark.sql import SparkSession

from easy_sql.sql_processor import SqlProcessor


def run_spark_etl():
    from easy_sql.sql_processor.backend import SparkBackend
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    backend = SparkBackend(spark)
    sql = '''
-- target=log.some_log
select 1 as a
    '''
    sql_processor = SqlProcessor(backend, sql)
    sql_processor.run()


def run_postgres_etl():
    from easy_sql.sql_processor.backend.rdb import RdbBackend
    backend = RdbBackend(os.environ['PG_URL'])
    sql = '''
-- target=log.some_log
select 1 as a
    '''
    sql_processor = SqlProcessor(backend, sql)
    sql_processor.run()


def run_clickhouse_etl():
    from easy_sql.sql_processor.backend.rdb import RdbBackend
    backend = RdbBackend(os.environ['CLICKHOUSE_URL'])
    sql = '''
-- target=log.some_log
select 1 as a
    '''
    sql_processor = SqlProcessor(backend, sql)
    sql_processor.run()


if __name__ == '__main__':
    run_spark_etl()
    run_postgres_etl()
    run_clickhouse_etl()
