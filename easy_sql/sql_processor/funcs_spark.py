from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Dict, List, Union

from .backend import SparkBackend
from .common import SqlProcessorAssertionError, _exec_sql, is_int_type
from .funcs_common import AlertFunc, AnalyticsFuncs, ColumnFuncs
from .funcs_common import IOFuncs as CommonIOFuncs
from .funcs_common import PartitionFuncs as PartitionFuncsBase
from .funcs_common import TableFuncs, TestFuncs

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import Row

    from easy_sql.sql_processor.context import ProcessorContext


__all__ = [
    "PartitionFuncs",
    "ColumnFuncs",
    "CacheFuncs",
    "ParallelismFuncs",
    "IOFuncs",
    "AlertFunc",
    "TableFuncs",
    "ModelFuncs",
    "AnalyticsFuncs",
    "LangFuncs",
    "TestFuncs",
]

from ..logger import logger


class ParallelismFuncs:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def repartition(self, table: str, partitions: str):
        try:
            _partitions = int(partitions)
        except ValueError:
            raise Exception(f"partitions must be an int when repartition a table, got `{partitions}`")
        _exec_sql(self.spark, f"select * from {table}").repartition(_partitions).createOrReplaceTempView(table)

    def repartition_by_column(self, table: str, partitions: str):
        _exec_sql(self.spark, f"select * from {table}").repartition(partitions).createOrReplaceTempView(table)

    def coalesce(self, table: str, partitions: str):
        try:
            _partitions = int(partitions)
        except ValueError:
            raise Exception(f"partitions must be an int when coalesce a table, got `{partitions}`")
        _exec_sql(self.spark, f"select * from {table}").coalesce(_partitions).createOrReplaceTempView(table)

    def set_shuffle_partitions(self, partitions: str):
        self.spark.conf.set("spark.sql.adaptive.enabled", "false")
        self.spark.conf.set("spark.sql.shuffle.partitions", partitions)


class IOFuncs(CommonIOFuncs):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write_csv(self, table: str, output_file: str):
        _exec_sql(self.spark, f"select * from {table}").repartition(1).write.mode("overwrite").csv(
            output_file, header=True
        )

    def rename_csv_output(self, spark_output_path: str, to_file: str):
        import re
        import subprocess

        assert to_file.startswith("/"), "to_file must be a full path starts with /, found: " + to_file
        is_local_file = spark_output_path.startswith("file:///")
        if is_local_file:
            command_prefix = ""
            spark_output_path = spark_output_path[len("file://") :]
        else:
            command_prefix = "hdfs dfs -"
        result = (
            subprocess.check_output(f"{command_prefix}ls {spark_output_path}".split(" ")).decode("utf8").split("\n")
        )
        generated_files = []
        for line in result:
            if line.endswith(".csv"):
                file_path = re.sub(r".*[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}", "", line).strip()
                # hdfs command will return full path, but system command will not, try fix to full path
                file_path = os.path.join(spark_output_path, file_path) if is_local_file else file_path
                generated_files.append(file_path)
        if not generated_files:
            raise Exception("no csv file found at path: " + spark_output_path)
        if len(generated_files) != 1:
            raise SqlProcessorAssertionError(
                f"found multiple csv file at path: {spark_output_path}, files={generated_files}"
            )
        subprocess.check_output(f"{command_prefix}mkdir -p {os.path.dirname(to_file)}".split(" "))
        try:
            subprocess.check_output(f"{command_prefix}rm {to_file}".split(" "))
        except Exception:
            logger.info(f"{to_file} not found, remove skipped")
        subprocess.check_output(f"{command_prefix}mv {generated_files[0]} {to_file}".split(" ")).decode("utf8").split(
            "\n"
        )

    def write_json_local(self, table: str, output_file: str):
        _data: DataFrame = _exec_sql(self.spark, f"select * from {table}")  # type: ignore
        data: List[Row] = _data.collect()  # type: ignore
        data: List[Dict] = [row.asDict() for row in data]  # type: ignore
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, "w") as f:
            f.write(json.dumps(data, ensure_ascii=False, indent=4, sort_keys=False))

    def update_json_local(self, context: ProcessorContext, vars: str, list_vars: str, json_attr: str, output_file: str):
        vars_value = {
            var.strip(): context.vars_context.vars.get(var.strip(), None) for var in vars.split(",") if var.strip()
        }
        list_vars_value = {
            var.strip(): context.vars_context.list_vars.get(var.strip(), None)
            for var in list_vars.split(",")
            if var.strip()
        }

        data = {}
        if os.path.exists(output_file):
            with open(output_file, "r") as f:
                data = json.loads(f.read())

        from easy_sql.utils.object_utils import get_attr

        data_current = get_attr(data, json_attr)
        data_current.update(vars_value)
        data_current.update(list_vars_value)

        with open(output_file, "w") as f:
            f.write(json.dumps(data, ensure_ascii=False, indent=4, sort_keys=False))


class ModelFuncs:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def model_predict(
        self, model_save_path: str, table_name: str, feature_cols: str, id_col: str, output_ref_cols: str
    ):
        from pyspark.ml import PipelineModel
        from pyspark.sql.functions import expr

        _output_ref_cols = [col.strip() for col in output_ref_cols.split(",") if col.strip()]
        model = PipelineModel.load(model_save_path)
        data = _exec_sql(self.spark, f"select {feature_cols} from {table_name}")

        int_cols = [f.name for f in data.schema.fields if is_int_type(f.dataType.typeName())]
        for col in int_cols:
            data = data.withColumn(col, expr(f"cast({col} as double)"))

        predictions = model.transform(data)
        output = predictions.select(_output_ref_cols + [id_col, "prediction"])
        output.createOrReplaceTempView(table_name)


class CacheFuncs:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def unpersist(self, table_name: str):
        self.spark.catalog.uncacheTable(table_name)


class PartitionFuncs(PartitionFuncsBase):
    def __init__(self, backend: Union[SparkSession, SparkBackend]):
        backend = backend if isinstance(backend, SparkBackend) else SparkBackend(backend)
        super().__init__(backend)

    def _get_partition_values(self, table_name):
        backend: SparkBackend = self.backend  # type: ignore
        partitions = _exec_sql(backend.spark, f"show partitions {table_name}").collect()
        partition_values = [p[0][p[0].index("=") + 1 :] for p in partitions]
        return partition_values

    def get_partition_cols(self, table_name: str) -> List[str]:
        backend: SparkBackend = self.backend  # type: ignore
        all_cols = _exec_sql(backend.spark, f"desc table {table_name}").collect()
        pt_cols = []
        pt_col_start = False
        for col in all_cols:
            if pt_col_start:
                pt_cols.append(col[0].strip())
            if col[0].strip() == "# col_name":
                pt_col_start = True
        return pt_cols


class LangFuncs:
    def __init__(self, backend: SparkBackend) -> None:
        self.backend = backend

    def call_java(self, cls: str, func_name: str, *args) -> str:
        spark = self.backend.spark
        gw = spark.sparkContext._gateway  # type: ignore
        from py4j.java_gateway import java_import

        java_import(gw.jvm, cls)
        func = eval(f"gw.jvm.{cls}.{func_name}", {"gw": gw})
        return func(spark._jsparkSession, *args)  # type: ignore
