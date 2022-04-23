import sys
from os import path
from typing import Dict, Any


src_path = path.dirname(path.abspath(__file__))
sys.path.insert(0, src_path)

__all__ = [
    'create_debugger', 'create_pg_debugger', 'create_ch_debugger'
]


def create_debugger(sql_file_path: str, vars: Dict[str, Any] = None, funcs: Dict[str, Any] = None):
    import os, subprocess
    spark_home = subprocess.check_output(['bash', '-c', "echo 'import os; print(os.environ[\"SPARK_HOME\"])' | pyspark"]).decode('utf8').split('\n')
    spark_home = [c.strip() for c in spark_home if c.strip()][0]
    os.environ['SPARK_HOME'] = spark_home
    import findspark
    findspark.init()

    from easy_sql.sql_processor.backend import SparkBackend
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    backend = SparkBackend(spark)
    from easy_sql.sql_processor_debugger import SqlProcessorDebugger
    debugger = SqlProcessorDebugger(sql_file_path, backend, vars, funcs)
    return debugger


def create_pg_debugger(sql_file_path: str, vars: Dict[str, Any] = None, funcs: Dict[str, Any] = None):
    from easy_sql.sql_processor.backend.rdb import RdbBackend
    pg = RdbBackend('postgresql://postgres:123456@testpg:15432/postgres')
    from easy_sql.sql_processor_debugger import SqlProcessorDebugger
    debugger = SqlProcessorDebugger(sql_file_path, pg, vars, funcs)
    return debugger


def create_ch_debugger(sql_file_path: str, vars: Dict[str, Any] = None, funcs: Dict[str, Any] = None):
    from easy_sql.sql_processor.backend.rdb import RdbBackend
    ch = RdbBackend('clickhouse+native://default@testch:30123')
    from easy_sql.sql_processor_debugger import SqlProcessorDebugger
    debugger = SqlProcessorDebugger(sql_file_path, ch, vars, funcs)
    return debugger
