from typing import Dict, Optional

from pyspark.sql import SparkSession


class SparkDynamicConfig:
    def __init__(self, max_shuffle_partitions: Optional[int] = None, min_shuffle_partitions: Optional[int] = None):
        self.max_shuffle_partitions = max_shuffle_partitions
        self.min_shuffle_partitions = min_shuffle_partitions

    def use_min_shuffle_partitions(self, spark: SparkSession) -> "SparkDynamicConfig":
        assert self.min_shuffle_partitions, "must provide min_shuffle_partitions to use the conf"
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.shuffle.partitions", str(self.min_shuffle_partitions))
        spark.conf.set("spark.default.parallelism", str(self.min_shuffle_partitions))
        return self

    def use_max_shuffle_partitions(self, spark: SparkSession) -> "SparkDynamicConfig":
        assert self.max_shuffle_partitions, "must provide max_shuffle_partitions to use the conf"
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.shuffle.partitions", str(self.max_shuffle_partitions))
        spark.conf.set("spark.default.parallelism", str(self.max_shuffle_partitions))
        return self

    def use_adaptive_shuffle_partitions(self, spark: SparkSession) -> "SparkDynamicConfig":
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        return self


def get_spark(app_name: str, conf: Optional[Dict] = None):
    builder = SparkSession.builder.appName(app_name).enableHiveSupport()
    conf = conf or {}
    for k, v in conf.items():
        builder.config(k, v)

    spark = builder.getOrCreate()
    spark.conf.set("spark.sql.statistics.fallBackToHdfs", "true")
    # 启用 Adaptive Execution ，从而启用自动设置 Shuffle Reducer 特性
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    # 设置每个 Reducer 读取的目标数据量，单位为字节。默认64M，一般改成集群块大小
    spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "134217728")
    # 允许动态资源分配，配合 spark.dynamicAllocation.minExecutors, spark.dynamicAllocation.maxExecutors 等使用
    # spark 3.0+ 不允许动态设置以下两个参数
    import pyspark

    if str(pyspark.__version__).startswith("2."):  # type: ignore
        spark.conf.set("spark.dynamicAllocation.enabled", "true")
        spark.conf.set("spark.shuffle.service.enabled", "true")

    # spark.conf.set("hive.exec.dynamic.partition", "true")
    # default strict. In strict mode, the user must specify at least one static partition,
    # in case the user accidentally overwrites all partitions.
    # In nonstrict mode all partitions are allowed to be dynamic.
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    return spark


def clear_temp_views(spark: SparkSession):
    for table in spark.catalog.listTables("default"):
        if table.isTemporary:
            print(f"dropping temp view {table.name}")
            spark.catalog.dropTempView(table.name)
