import json
import os
from typing import List

from .backend.rdb import RdbBackend
from .funcs_common import ColumnFuncs, TableFuncs, PartitionFuncs as PartitionFuncsBase, AlertFunc

__all__ = [
    'PartitionFuncs', 'ColumnFuncs', 'AlertFunc', 'TableFuncs', 'ModelFuncs'
]


class ModelFuncs:

    def __init__(self, backend: RdbBackend):
        self.backend = backend

    def bq_model_predict_with_local_spark(self, model_save_path: str, input_table_name: str, output_table_name: str,
                                          feature_cols: str, id_col: str, output_ref_cols: str):
        from pyspark.ml import PipelineModel
        from pyspark.sql.functions import expr
        from ..spark_optimizer import get_spark

        SPARK_JARS = "/tmp/app/dataplat/lib/scala/lib_spark3/spark-bigquery-latest_2.12.jar," \
                     "/tmp/app/dataplat/lib/scala/lib_spark3/gcs-connector-hadoop2-latest.jar,"

        spark_config_settings_dict = {
            'spark.jars': SPARK_JARS,
            'spark.master': 'local[2]',
            'spark.submit.deployMode': 'client',
            'spark.executor.memory': '1g',
            'spark.executor.cores': '1',
            'spark.hadoop.fs.AbstractFileSystem.gs.impl': 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS',
            'parentProject': 'data-dev-workbench-prod-3fd9',
            'spark.sql.warehouse.dir': '/tmp/spark-warehouse-localdw',
            'spark.driver.extraJavaOptions': "-Dderby.system.home=/tmp/spark-warehouse-metastore "
                                             "-Dderby.stream.error.file=/tmp/spark-warehouse-metastore.log",
            "credentialsFile": f"{os.environ.get('HOME', '/tmp')}/.bigquery/credential-prod.json"
        }

        spark = get_spark(f'test', spark_config_settings_dict)
        bucket = "dataplat-gcp-demo"
        spark.conf.set('temporaryGcsBucket', bucket)

        spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
        spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
        spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile',
                                             f"{os.environ.get('HOME', '/tmp')}/.bigquery/credential-prod.json")

        spark.read.format('bigquery') \
            .option("parentProject", 'data-dev-workbench-prod-3fd9') \
            .option("table", input_table_name) \
            .load() \
            .createOrReplaceTempView("origin_input")

        data = spark.sql(f'select {feature_cols} from origin_input')

        output_ref_cols = [col.strip() for col in output_ref_cols.split(',') if col.strip()]
        model = PipelineModel.load(model_save_path)

        is_int_type = lambda type_name: any([type_name.startswith(t) for t in ['integer', 'long', 'decimal', 'short']])
        int_cols = [f.name for f in data.schema.fields if is_int_type(f.dataType.typeName())]
        for col in int_cols:
            data = data.withColumn(col, expr(f'cast({col} as double)'))

        predictions = model.transform(data)
        output = predictions.select(output_ref_cols + [id_col, 'prediction'])

        output.write.format('bigquery') \
            .option("parentProject", 'data-dev-workbench-prod-3fd9') \
            .option('table', output_table_name) \
            .mode("overwrite") \
            .save()

    def model_predict_with_local_spark(self, model_save_path: str, input_table_name: str, output_table_name: str,
                                       feature_cols: str, id_col: str, output_ref_cols: str):
        from pyspark.ml import PipelineModel
        from pyspark.sql.functions import expr
        from ..spark_optimizer import get_spark

        workdir = os.path.dirname(os.environ.get('PWD'))
        local_settings_dir = f"{workdir}/settings.local.json"
        settings_dir = f"{workdir}/settings.json"

        with open(settings_dir, 'r') as json_file:
            settings = json.load(json_file)
            data_source_load_options = settings["ml_model"]["data_source_load_options"]
            data_type_map = settings["ml_model"]["data_type_map"]
            SPARK_JARS = "".join(settings["ml_model"]["spark_jars"])

        with open(local_settings_dir, 'r') as json_file:
            local_settings = json.load(json_file)
            local_data_source_load_options = local_settings["ml_model"]["data_source_load_options"]

        file_dir = os.path.dirname(os.path.abspath(__file__))

        spark_config_settings_dict = {
            'spark.jars': SPARK_JARS,
            'spark.master': 'local[2]',
            'spark.submit.deployMode': 'client',
            'spark.executor.memory': '1g',
            'spark.executor.cores': '1',
            'spark.sql.warehouse.dir': '/tmp/spark-warehouse-localdw',
            'spark.driver.extraJavaOptions': "-Dderby.system.home=/tmp/spark-warehouse-metastore "
                                             "-Dderby.stream.error.file=/tmp/spark-warehouse-metastore.log",
            'spark.driver.extraClassPath': os.path.join(file_dir, '../../../deps/lib/*')

        }

        from py4j.java_gateway import java_import
        spark = get_spark("ml_local", spark_config_settings_dict)
        gw = spark.sparkContext._gateway
        java_import(gw.jvm, "common.dataplat.sparkutils")

        data = spark.read.format(f"{data_source_load_options['format']}") \
            .option("driver", f"{data_source_load_options['driver']}") \
            .option("url", f"{data_source_load_options['url']}") \
            .option("user", f"{data_source_load_options['user']}") \
            .option("password", f"{local_data_source_load_options['password']}") \
            .option("dbtable", input_table_name) \
            .load()

        output_ref_cols = [col.strip() for col in output_ref_cols.split(',') if col.strip()]
        model = PipelineModel.load(model_save_path)

        is_int_type = lambda type_name: any([type_name.startswith(t) for t in ['integer', 'long', 'decimal', 'short']])
        int_cols = [f.name for f in data.schema.fields if is_int_type(f.dataType.typeName())]
        for col in int_cols:
            data = data.withColumn(col, expr(f'cast({col} as double)'))

        predictions = model.transform(data)
        output = predictions.select(output_ref_cols + [id_col, 'prediction'])

        ddl = self.__get_ddl_by_backend(data_type_map, output_table_name, id_col, output.dtypes)

        self.backend.exec_native_sql(ddl)
        self.backend.exec_native_sql(f"truncate table {output_table_name}")
        output.write.format(f"{data_source_load_options['format']}") \
            .mode("append") \
            .option("driver", f"{data_source_load_options['driver']}") \
            .option("truncate", f"{data_source_load_options['truncate']}") \
            .option("url", f"{data_source_load_options['url']}") \
            .option("user", f"{data_source_load_options['user']}") \
            .option("password", f"{local_data_source_load_options['password']}") \
            .option("dbtable", output_table_name) \
            .save()

    def __get_ddl_by_backend(self, data_type_map, output_table_name, id_col, output_dtypes) -> str:
        if self.backend.is_postgres_backend:
            columns_def = ', '.join([f'{col} {data_type_map[col_type]} {"PRIMARY KEY" if col == id_col else ""}' for col, col_type in output_dtypes])
            return f"CREATE TABLE IF NOT EXISTS {output_table_name} ({columns_def})"
        elif self.backend.is_clickhouse_backend:
            columns_def = ', '.join([f'{col} {data_type_map[col_type]}' for col, col_type in output_dtypes])
            return f"""CREATE TABLE IF NOT EXISTS {output_table_name} ({columns_def}) engine = MergeTree
            PRIMARY KEY {id_col}
            ORDER BY {id_col}"""
        else:
            msg = f'Backend of type {type(self.backend)}-{self.backend.backend_type if isinstance(self.backend, RdbBackend) else ""} ' \
                  f'is not supported yet'
            raise Exception(msg)


class PartitionFuncs(PartitionFuncsBase):

    def __check_backend(self):
        if not isinstance(self.backend, RdbBackend):
            msg = f'Backend of type {type(self.backend)}-{self.backend.backend_type if isinstance(self.backend, RdbBackend) else ""} ' \
                  f'is not supported yet'
            raise Exception(msg)

    def _get_bigquery_partition_values(self, table_name):
        db, table = self.__parse_table_name(table_name)
        sql = f"select distinct partition_value from {db}.__table_partitions__ where table_name = '{table}' order by partition_value"
        partition_values = [str(v[0]) for v in self.backend.exec_sql(sql).collect()]
        return partition_values

    def _get_clickhouse_partition_values(self, table_name):
        db, table = self.__parse_table_name(table_name)
        sql = f"SELECT distinct partition_value FROM {self.backend.partitions_table_name} where db_name = '{db}' and table_name = '{table}';"
        partition_values = [str(v[0]) for v in self.backend.exec_sql(sql).collect()]
        partition_values.sort()
        return partition_values

    def _get_postgresql_partition_values(self, table_name):
        db, table = self.__parse_table_name(table_name)
        sql = f'''
        SELECT
            concat(nmsp_child.nspname, '.', child.relname) as partition_tables,
            pg_catalog.pg_get_expr(child.relpartbound, child.oid) as partition_expr
        FROM pg_inherits
            JOIN pg_class parent        ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child         ON pg_inherits.inhrelid   = child.oid
            JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
            JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
            JOIN pg_partitioned_table part  ON part.partrelid = parent.oid
        WHERE nmsp_parent.nspname='{db}' and parent.relname='{table}'
        '''
        partition_values = [str(v[1]) for v in self.backend.exec_sql(sql).collect()]
        for p in partition_values:
            if not p.upper().startswith('FOR VALUES FROM (') or not ') TO (' in p.upper():
                raise Exception('unable to parse partition: ' + p)
        partition_values = [v[len('FOR VALUES FROM ('):v.upper().index(') TO (')] for v in partition_values]
        partition_values = [v.strip("'") if v.startswith("'") else int(v) for v in partition_values]
        partition_values.sort()
        return partition_values

    def _get_partition_values(self, table_name):
        self.__check_backend()
        if self.backend.is_pg:
            return self._get_postgresql_partition_values(table_name)
        elif self.backend.is_ch:
            return self._get_clickhouse_partition_values(table_name)
        elif self.backend.is_bq:
            return self._get_bigquery_partition_values(table_name)
        else:
            msg = f'Backend of type {type(self.backend)}-{self.backend.backend_type if isinstance(self.backend, RdbBackend) else ""} ' \
                  f'is not supported yet'
            raise Exception(msg)

    def get_partition_cols(self, table_name: str) -> List[str]:
        self.__check_backend()
        db, table = self.__parse_table_name(table_name)
        if isinstance(self.backend, RdbBackend):
            native_partitions_sql, extract_partition_cols = self.backend.sql_dialect.native_partitions_sql(f'{db}.{table}')
            pt_cols = extract_partition_cols(self.backend.exec_native_sql(native_partitions_sql))
            return pt_cols
        else:
            raise AssertionError('should not happen!')

    def __parse_table_name(self, table_name):
        backend: RdbBackend = self.backend
        full_table_name = table_name if '.' in table_name else f'{backend.temp_schema}.{table_name}'
        db, table = full_table_name[:full_table_name.index('.')], full_table_name[full_table_name.index('.') + 1:]
        return db, table
