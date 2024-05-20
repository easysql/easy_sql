from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Union

from ...logger import logger
from ...udf import udfs
from ..common import SqlProcessorAssertionError, SqlProcessorException
from .base import Backend, Col, Partition, Row, SaveMode, Table, TableMeta

__all__ = ["SparkRow", "SparkTable", "SparkBackend"]

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


class SparkRow(Row):
    def __init__(self, row):
        from pyspark.sql.types import Row as SparkRow

        self.row: SparkRow = row

    def as_dict(self):
        return self.row.asDict()  # type: ignore

    def as_tuple(self) -> Tuple:
        return self.row  # type: ignore

    def __eq__(self, other):
        return self.row.__eq__(other)

    def __str__(self):
        return str(self.row)[4:-1]

    def __getitem__(self, i):
        return self.row[i]

    def __repr__(self):
        return self.row.__repr__()


class SparkTable(Table):
    def __init__(self, df):
        from pyspark.sql import DataFrame

        self.df: DataFrame = df

    def is_empty(self) -> bool:
        return self.df.count() == 0

    def field_names(self) -> List[str]:
        return self.df.schema.fieldNames()

    def first(self) -> Row:
        return SparkRow(self.df.first())

    def limit(self, count: int) -> SparkTable:
        return SparkTable(self.df.limit(count))

    def with_column(self, name: str, value: Any) -> SparkTable:
        from pyspark.sql import Column
        from pyspark.sql.functions import expr

        return SparkTable(self.df.withColumn(name, value if isinstance(value, Column) else expr(value)))

    def collect(self) -> List[Row]:
        return [SparkRow(row) for row in self.df.collect()]

    def show(self, count: int = 20):
        self.df.show(count)

    def count(self) -> int:
        return self.df.count()


class SparkBackend(Backend):
    def __init__(self, spark, scala_udf_initializer: Optional[str] = None):
        from pyspark.sql import SparkSession

        self.spark: SparkSession = spark
        self.scala_udf_initializer = scala_udf_initializer
        self.save_table_start_hooks: List[Callable[[SparkBackend, TableMeta, TableMeta], None]] = []
        self.save_table_end_hooks: List[Callable[[SparkBackend, TableMeta], None]] = []

    def reset(self):
        pass

    def init_udfs(self, scala_udf_initializer: Optional[str] = None, *args, **kwargs):
        scala_udf_initializer = scala_udf_initializer or self.scala_udf_initializer
        if scala_udf_initializer:
            from py4j.java_gateway import java_import

            gw = self.spark.sparkContext._gateway  # type: ignore
            java_import(gw.jvm, scala_udf_initializer)
            initUdfs = eval(f"gw.jvm.{scala_udf_initializer}.initUdfs", {"gw": gw})
            initUdfs(self.spark._jsparkSession)  # type: ignore

        self.register_udfs(udfs.get_udfs("spark"))

    def register_udfs(self, funcs: Dict[str, Callable]):
        for key in funcs:
            func = funcs[key]
            self.spark.udf.register(key, func)

    def set_spark_configs(self, configs: Dict[str, str]):
        for key, value in configs.items():
            self.spark.conf.set(key, value)

    def temp_tables(self) -> List[str]:
        return [table.name for table in self.spark.catalog.listTables("default") if table.isTemporary]

    def clear_cache(self):
        self.spark.catalog.clearCache()

    def clear_temp_tables(self, exclude: Optional[List[str]] = None):
        exclude = exclude or []
        for table in self.spark.catalog.listTables("default"):
            if table.isTemporary and table.name not in exclude:
                logger.info(f"dropping temp view {table.name}")
                self.spark.catalog.dropTempView(table.name)

    def create_empty_table(self):
        from pyspark.sql.types import StructType

        return SparkTable(self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), StructType([])))

    def create_temp_table(self, table: SparkTable, name: str):
        table.df.createOrReplaceTempView(name)

    def create_cache_table(self, table: SparkTable, name: str):
        table.df.createOrReplaceTempView(name)
        logger.info(f"caching table: {name}")
        self.spark.catalog.cacheTable(name)

    def broadcast_table(self, table: SparkTable, name: str):
        from pyspark.sql.functions import broadcast

        df = broadcast(table.df)
        df.createOrReplaceTempView(name)

    def exec_native_sql(self, sql: str) -> DataFrame:
        logger.info(f"will exec sql: {sql}")
        return self.spark.sql(sql)

    def exec_sql(self, sql: str) -> Table:
        logger.info(f"will exec sql: {sql}")
        return SparkTable(self.spark.sql(sql))

    def table_exists(self, table: TableMeta):
        from pyspark.sql.utils import AnalysisException

        try:
            return self.spark._jsparkSession.catalog().tableExists(table.dbname, table.pure_table_name)  # type: ignore
        except AnalysisException:
            return False

    def _create_table(self, dbname: str, table_name: str, schema: StructType, partitions: List[Partition]):
        from pyspark.sql.functions import lit

        spark = self.spark
        df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
        schema = df.schema
        for p in partitions or []:
            if p.field not in schema.fieldNames():
                if p.value is None:
                    raise SqlProcessorAssertionError(
                        "partition column value is None when create table with partitions but partitions is not in"
                        f" dataframe. this should not happen. table_name={dbname}.{table_name}, p.field={p.field},"
                        f" p.value={p.value}"
                    )
                df = df.withColumn(p.field, lit(p.value))

        temp_view_name = f"{dbname}__{table_name}__table_data"
        df.createOrReplaceTempView(temp_view_name)
        partition_expr = f'partitioned by ({",".join([p.field for p in partitions])}) ' if partitions else ""
        create_database_stmt = f"create database if not exists {dbname}"
        create_table_stmt = f"""create table if not exists {dbname}.{table_name} using hive
                                options(FILEFORMAT "parquet") {partition_expr}
                                TBLPROPERTIES ("transactional" = "false")
                                as select * from {temp_view_name}"""

        self.exec_native_sql(create_database_stmt)
        self.exec_native_sql(create_table_stmt)
        return self

    def verify_schema(self, source_table: TableMeta, target_table: TableMeta, verify_type: bool = False):
        if not self.table_exists(target_table):
            raise SqlProcessorException(f"Verify schema failed. Target table {target_table.table_name} does not exist")
        source_columns = self.exec_native_sql(f"select * from {source_table.table_name}").schema.fields
        target_columns = self.exec_native_sql(f"select * from {target_table.table_name}").schema.fields
        source_col_names = {c.name.lower() for c in source_columns}.union(
            {p.field.lower() for p in source_table.partitions}
        )
        target_col_names = {c.name.lower() for c in target_columns}
        if not target_col_names.issubset(source_col_names):
            raise SqlProcessorException(
                f"Target table {target_table.table_name} has columns that are not in source table"
                f" {source_table.table_name}: {target_col_names - source_col_names}"
            )
        source_col = lambda col_name: next(c for c in source_columns if c.name.lower() == col_name.lower())
        type_diff_cols = [
            f"{c.name.lower()}(target_type={c.dataType}, source_type={source_col(c.name).dataType})"
            for c in target_columns
            if c.name.lower() in source_col_names and c.dataType != source_col(c.name).dataType
        ]
        # will not verify partition column types since it will be converted automatically when saving.
        if type_diff_cols:
            message = (
                f"target table {target_table.table_name} has columns whose type are different from source table"
                f" {source_table.table_name}: " + ", ".join(type_diff_cols)
            )
            if verify_type:
                raise SqlProcessorException("Verify schema failed. Found " + message)
            else:
                logger.info("Verify schema passed. Found " + message)
        else:
            logger.info(
                "Verify schema passed. Source table has all columns in target table and their types are the same."
            )

    def register_save_table_start_hooks(self, hooks: List[Callable[[SparkBackend, TableMeta, TableMeta], None]]):
        self.save_table_start_hooks.extend(hooks)

    def register_save_table_end_hooks(self, hooks: List[Callable[[SparkBackend, TableMeta], None]]):
        self.save_table_end_hooks.extend(hooks)

    def _call_save_table_start_hooks(self, source_table: TableMeta, target_table: TableMeta):
        for hook in self.save_table_start_hooks:
            start_time = time.time()
            logger.info(f"calling save table start hook start: {hook.__name__}")
            hook(self, source_table, target_table)
            logger.info(
                f"calling save table start hook end(time took {time.time() - start_time:.2f}s): {hook.__name__}"
            )

    def _call_save_table_end_hooks(self, target_table: TableMeta):
        for hook in self.save_table_end_hooks:
            start_time = time.time()
            logger.info(f"calling save table end hook start: {hook.__name__}")
            hook(self, target_table)
            logger.info(f"calling save table end hook end(time took {time.time() - start_time:.2f}s): {hook.__name__}")

    def save_table_sql(self, source_table: TableMeta, source_table_sql: str, target_table: TableMeta) -> str:
        columns = self.exec_native_sql(f"select * from {source_table.table_name}").limit(0).columns
        return f'insert into {target_table.table_name} select {",".join(columns)} from ({source_table_sql})'

    def save_table(
        self,
        source_table_meta: TableMeta,
        target_table_meta: TableMeta,
        save_mode: SaveMode,
        create_target_table: bool,
    ):
        from pyspark.sql.functions import lit

        self._call_save_table_start_hooks(source_table_meta, target_table_meta)

        if not self.table_exists(target_table_meta) and create_target_table:
            schema = self.spark.sql(f"select * from {source_table_meta.table_name}").limit(0).schema
            assert target_table_meta.dbname is not None
            self._create_table(
                target_table_meta.dbname, target_table_meta.pure_table_name, schema, target_table_meta.partitions
            )

        temp_res = self.exec_native_sql(f"select * from {source_table_meta.table_name}")
        # partial dynamic partition (动态分区和静态分区同时使用）在 spark 2.3.2 上有问题，参见 https://issues.apache.org/jira/browse/SPARK-31605
        # 纯动态分区时，如果当日没有新增数据，则不会创建 partition。而我们希望对于静态分区，总是应该创建分区，即使当日没有数据
        dynamic_partitions = list(filter(lambda p: not p.value, target_table_meta.partitions))
        static_partitions = list(filter(lambda p: p.value, target_table_meta.partitions))
        columns = self.exec_native_sql(f"select * from {target_table_meta.table_name}").limit(0).columns
        if dynamic_partitions:
            for p in static_partitions:
                temp_res = temp_res.withColumn(p.field, lit(p.value))
            temp_res = temp_res.select(*columns)
            fields = [f"{p.field}" for p in dynamic_partitions]
        else:
            columns = [c for c in columns if c not in [p.field for p in static_partitions]]
            temp_res = temp_res.select(*columns)
            fields = [
                f"{p.field}='{p.value}'" if isinstance(p.value, str) else f"{p.field}={p.value}"
                for p in target_table_meta.partitions
            ]
        partition_expr = f"partition ({','.join(fields)})" if fields else ""

        def __save_data(temp_res: DataFrame):
            temp_res_name = f"{source_table_meta.pure_table_name}__result__{id(temp_res)}"
            temp_res.createOrReplaceTempView(temp_res_name)
            save_sql = (
                f"insert {'into' if save_mode == SaveMode.append else save_mode.name} table"
                f" {target_table_meta.table_name} {partition_expr} select * from {temp_res_name}"
            )
            self.exec_native_sql(save_sql)

        try:
            __save_data(temp_res)
        except Exception as e:
            if str(e).strip() == "Cannot overwrite a path that is also being read from.":
                # to resolve issue: pyspark.sql.utils.AnalysisException: Cannot overwrite a path that is also being read from.
                # refer: https://stackoverflow.com/questions/38746773/read-from-a-hive-table-and-write-back-to-it-using-spark-sql
                logger.info(
                    f"Save data failed (from {source_table_meta.pure_table_name} to {target_table_meta.table_name}) because of "
                    '"Cannot overwrite a path that is also being read from.". Will try create a dataframe from rdd to avoid this. '
                    "This will break the dataframe connection and might cause issue in spark ui."
                )
                temp_res = self.spark.createDataFrame(temp_res.rdd, temp_res.schema)
                __save_data(temp_res)
            else:
                raise e
        self._call_save_table_end_hooks(target_table_meta)

    def refresh_table_partitions(self, table: TableMeta):
        df = self.exec_native_sql(f"desc {table.table_name}")
        column_list = df.select(df.col_name, df.data_type).rdd.map(lambda x: (x[0], x[1])).collect()
        partition_details = [
            column_list[index + 1 :] for index, item in enumerate(column_list) if item[0] == "# col_name"
        ]
        if len(partition_details) == 0:
            table.update_partitions([])
        else:
            partitions = [Partition(x[0]) for x in partition_details[0]]
            table.update_partitions([Partition(p.field, p.value) for p in partitions])

    def clean(self, dry_run: bool = False):
        from easy_sql.spark_optimizer import clear_temp_views

        self.spark.catalog.clearCache()
        clear_temp_views(self.spark)

    def create_table_with_data(
        self,
        full_table_name: str,
        values: List[List[Any]],
        schema: Union[StructType, List[Col]],
        partitions: List[Partition],
    ):
        print(f"creating table: {full_table_name}")
        self.spark.sql(f'create database if not exists {full_table_name.split(".")[0]}')
        self.spark.sql(f"drop table if exists {full_table_name}").collect()
        from pyspark.sql.types import StructType

        schema_or_cols = schema if isinstance(schema, StructType) else [col.name for col in schema]
        write = self.spark.createDataFrame(values, schema_or_cols).write
        if partitions:
            write = write.partitionBy(*[p.field for p in partitions])
        write.mode("overwrite").saveAsTable(full_table_name, mode="overwrite")

    def create_temp_table_with_data(self, table_name: str, values: List[List[Any]], schema: StructType):
        self.spark.createDataFrame(values, schema).createOrReplaceTempView(table_name)
