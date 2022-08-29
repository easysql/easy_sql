import os
import shutil
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession


class LocalSpark:
    spark: Optional[SparkSession] = None
    __conf: Dict = {}

    @staticmethod
    def stop():
        if LocalSpark.spark:
            LocalSpark.spark.stop()
            LocalSpark.spark = None

    @staticmethod
    def get(conf: Optional[Dict[str, Any]] = None, clean_existing_data: bool = True) -> SparkSession:
        conf = conf or {}
        if LocalSpark.spark is None:
            default_conf = {
                "spark.default.parallelism": 4,
                "hive.exec.dynamic.partition.mode": "nonstrict",
                "spark.sql.warehouse.dir": "/tmp/spark-warehouse-localdw-ut",
                "spark.driver.extraJavaOptions": (
                    "-Dderby.system.home=/tmp/spark-warehouse-metastore-ut "
                    "-Dderby.stream.error.file=/tmp/spark-warehouse-metastore-ut.log"
                ),
            }
            default_conf.update(conf)
            conf = default_conf

            if clean_existing_data:
                # delete old spark warehouse/metastore dir
                print(f"removing dir {conf['spark.sql.warehouse.dir']}")
                shutil.rmtree(conf["spark.sql.warehouse.dir"], ignore_errors=True)
                if "-Dderby.system.home" in conf["spark.driver.extraJavaOptions"]:
                    import re

                    java_options = re.sub(r"\s*=\s*", "=", conf["spark.driver.extraJavaOptions"].strip()).split()
                    for op in java_options:
                        if op.split("=")[0].strip() == "-Dderby.system.home":
                            print(f"removing dir {op.split('=')[1].strip()}")
                            shutil.rmtree(op.split("=")[1].strip(), ignore_errors=True)

            # ensure a local spark with default config
            os.environ["SPARK_CONF_DIR"] = "/tmp/local-spark-conf-ut"
            spark_builder = SparkSession.builder.appName("UnitTest").master("local[4]")

            print("using conf: ", conf)
            for k, v in conf.items():
                spark_builder.config(k, v)
            LocalSpark.spark = spark_builder.enableHiveSupport().getOrCreate()

        spark = LocalSpark.spark
        spark.catalog.clearCache()
        for table in spark.catalog.listTables("default"):
            if table.isTemporary:
                print(f"dropping temp view {table.name}")
                spark.catalog.dropTempView(table.name)

        return LocalSpark.spark
