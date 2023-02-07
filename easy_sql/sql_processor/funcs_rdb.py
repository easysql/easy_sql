from __future__ import annotations

import json
import os
import typing
from typing import Any, Dict, List

if typing.TYPE_CHECKING:
    from pyspark.sql import DataFrame

from ..logger import logger
from .backend.rdb import RdbBackend
from .common import is_int_type
from .funcs_common import AlertFunc, AnalyticsFuncs, ColumnFuncs, IOFuncs
from .funcs_common import PartitionFuncs as PartitionFuncsBase
from .funcs_common import TableFuncs, TestFuncs

__all__ = [
    "PartitionFuncs",
    "ColumnFuncs",
    "AlertFunc",
    "TableFuncs",
    "ModelFuncs",
    "IOFuncs",
    "AnalyticsFuncs",
    "TestFuncs",
]


class Settings:
    def __init__(self, root_key: str) -> None:
        workdir = os.path.dirname(os.environ.get("PWD"))  # type: ignore
        self.local_settings_file = f"{workdir}/settings.local.json"
        self.settings_file = f"{workdir}/settings.json"
        self.root_key = root_key

        with open(self.settings_file, "r") as json_file:
            self.settings = json.load(json_file)

        with open(self.local_settings_file, "r") as json_file:
            self.local_settings = json.load(json_file)

    def prioritized_setting(self, key: str, default_value: Any = None, required: bool = True) -> Any:
        v = self.local_settings.get(self.root_key, {}).get(
            key, self.settings.get(self.root_key, {}).get(key, default_value)
        )
        if default_value is None and v is None and required:
            raise Exception(
                f"Required configuration `{self.root_key}.{key}` not found or is null in file {self.settings_file} or"
                f" {self.local_settings_file}"
            )
        return v


class ModelFuncs:
    def __init__(self, backend: RdbBackend):
        self.backend = backend

    def bq_model_predict_with_local_spark(
        self,
        model_save_path: str,
        input_table_name: str,
        output_table_name: str,
        feature_cols: str,
        id_col: str,
        output_ref_cols: str,
    ):
        from ..spark_optimizer import get_spark

        settings = Settings("ml_model")

        spark_config_settings_dict: Dict[str, str] = settings.prioritized_setting("spark_conf")
        home = os.environ.get("HOME", "/tmp")
        spark_config_settings_dict = {k: v.replace("${HOME}", home) for k, v in spark_config_settings_dict}

        spark = get_spark("ml_local", spark_config_settings_dict)
        bucket = settings.prioritized_setting("bucket")
        spark.conf.set("temporaryGcsBucket", bucket)

        spark._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")  # type: ignore
        spark._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.enable", "true")  # type: ignore
        spark._jsc.hadoopConfiguration().set(  # type: ignore
            "google.cloud.auth.service.account.json.keyfile",
            settings.prioritized_setting("keyfile").replace("${HOME}", home),
        )

        parentProject = settings.prioritized_setting("parentProject")
        spark.read.format("bigquery").option("parentProject", parentProject).option(
            "table", input_table_name
        ).load().createOrReplaceTempView("__origin_input")

        data = spark.sql(f"select {feature_cols} from __origin_input")

        output = self._predict(model_save_path, id_col, output_ref_cols, data)

        writer = (
            output.write.format("bigquery")
            .option("parentProject", parentProject)
            .option("table", output_table_name)
            .mode("overwrite")
        )
        writer.save()

    def _predict(self, model_save_path: str, id_col: str, output_ref_cols: str, data: DataFrame):
        from pyspark.ml import PipelineModel
        from pyspark.sql.functions import expr

        model = PipelineModel.load(model_save_path)

        int_cols = [f.name for f in data.schema.fields if is_int_type(f.dataType.typeName())]
        for col in int_cols:
            data = data.withColumn(col, expr(f"cast({col} as double)"))

        predictions = model.transform(data)

        _output_ref_cols = [col.strip() for col in output_ref_cols.split(",") if col.strip()]
        output_cols = _output_ref_cols + [id_col, "prediction"]
        logger.info(f"output cols: {output_cols}, prediction cols: {predictions.dtypes}")
        output = predictions.select(output_cols)

        return output

    def model_predict_with_local_spark(
        self,
        model_save_path: str,
        input_table_name: str,
        output_table_name: str,
        feature_cols: str,
        id_col: str,
        output_ref_cols: str,
    ):
        from ..spark_optimizer import get_spark

        settings = Settings("ml_model")
        spark_config_settings_dict = settings.prioritized_setting("spark_conf")
        spark = get_spark("ml_local", spark_config_settings_dict)

        data_source = settings.prioritized_setting("data_source")
        data = (
            spark.read.format(f"{data_source['format']}")
            .option("driver", f"{data_source['driver']}")
            .option("url", f"{data_source['url']}")
            .option("user", f"{data_source['user']}")
            .option("password", f"{data_source['password']}")
            .option("dbtable", input_table_name)
            .load()
            .createOrReplaceTempView("__origin_input")
        )
        data = spark.sql(f"select {feature_cols} from __origin_input")

        output = self._predict(model_save_path, id_col, output_ref_cols, data)

        data_type_map = settings.prioritized_setting("data_type_map", required=False)
        ddl = self.__get_ddl_by_backend(data_type_map, output_table_name, id_col, output.dtypes)
        self.backend.exec_native_sql(ddl)
        self.backend.exec_native_sql(f"truncate table {output_table_name}")

        writer = (
            output.write.format(f"{data_source['format']}")
            .mode("append")
            .option("driver", f"{data_source['driver']}")
            .option("truncate", f"{data_source['truncate']}")
            .option("url", f"{data_source['url']}")
            .option("user", f"{data_source['user']}")
            .option("password", f"{data_source['password']}")
            .option("dbtable", output_table_name)
        )
        writer.save()

    def __get_ddl_by_backend(self, data_type_map, output_table_name, id_col, output_dtypes) -> str:
        if self.backend.is_postgres_backend:
            columns_def = ", ".join(
                [
                    f'{col} {data_type_map[col_type]} {"PRIMARY KEY" if col == id_col else ""}'
                    for col, col_type in output_dtypes
                ]
            )
            return f"CREATE TABLE IF NOT EXISTS {output_table_name} ({columns_def})"
        elif self.backend.is_clickhouse_backend:
            columns_def = ", ".join([f"{col} {data_type_map[col_type]}" for col, col_type in output_dtypes])
            return f"""CREATE TABLE IF NOT EXISTS {output_table_name} ({columns_def}) engine = MergeTree
            PRIMARY KEY {id_col}
            ORDER BY {id_col}"""
        else:
            msg = (
                "Backend of type"
                f' {type(self.backend)}-{self.backend.backend_type if isinstance(self.backend, RdbBackend) else ""} is'
                " not supported yet"
            )
            raise Exception(msg)


class PartitionFuncs(PartitionFuncsBase):
    def __check_backend(self):
        if not isinstance(self.backend, RdbBackend):
            msg = (
                "Backend of type"
                f' {type(self.backend)}-{self.backend.backend_type if isinstance(self.backend, RdbBackend) else ""} is'
                " not supported yet"
            )
            raise Exception(msg)

    def _get_bigquery_partition_values(self, table_name):
        db, table = self.__parse_table_name(table_name)
        sql = (
            f"select distinct partition_value from {db}.__table_partitions__ where table_name = '{table}' order by"
            " partition_value"
        )
        partition_values = [str(v[0]) for v in self.backend.exec_sql(sql).collect()]
        return partition_values

    def _get_clickhouse_partition_values(self, table_name):
        db, table = self.__parse_table_name(table_name)
        sql = (
            f"SELECT distinct partition_value FROM {self.backend.partitions_table_name} where db_name = '{db}' and"  # type: ignore
            f" table_name = '{table}';"
        )
        partition_values = [str(v[0]) for v in self.backend.exec_sql(sql).collect()]
        partition_values.sort()
        return partition_values

    def _get_postgresql_partition_values(self, table_name):
        db, table = self.__parse_table_name(table_name)
        sql = f"""
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
        """
        partition_values = [str(v[1]) for v in self.backend.exec_sql(sql).collect()]
        for p in partition_values:
            if not p.upper().startswith("FOR VALUES FROM (") or ") TO (" not in p.upper():
                raise Exception("unable to parse partition: " + p)
        partition_values = [v[len("FOR VALUES FROM (") : v.upper().index(") TO (")] for v in partition_values]
        partition_values = [v.strip("'") if v.startswith("'") else int(v) for v in partition_values]
        partition_values.sort()
        return partition_values

    def _get_partition_values(self, table_name):
        self.__check_backend()
        backend: RdbBackend = self.backend  # type: ignore
        if backend.is_pg:
            return self._get_postgresql_partition_values(table_name)
        elif backend.is_ch:
            return self._get_clickhouse_partition_values(table_name)
        elif backend.is_bq:
            return self._get_bigquery_partition_values(table_name)
        else:
            msg = (
                "Backend of type"
                f' {type(backend)}-{backend.backend_type if isinstance(backend, RdbBackend) else ""} is'
                " not supported yet"
            )
            raise Exception(msg)

    def get_partition_cols(self, table_name: str) -> List[str]:
        self.__check_backend()
        backend: RdbBackend = self.backend  # type: ignore
        db, table = self.__parse_table_name(table_name)
        native_partitions_sql, extract_partition_cols = backend.sql_dialect.native_partitions_sql(f"{db}.{table}")
        pt_cols = extract_partition_cols(self.backend.exec_native_sql(native_partitions_sql))
        return pt_cols

    def __parse_table_name(self, table_name):
        backend: RdbBackend = self.backend  # type: ignore
        full_table_name = table_name if "." in table_name else f"{backend.temp_schema}.{table_name}"
        db, table = full_table_name[: full_table_name.index(".")], full_table_name[full_table_name.index(".") + 1 :]
        return db, table
