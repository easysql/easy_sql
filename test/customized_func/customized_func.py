__all__ = ['count_partitions']


def count_partitions(table_name: str) -> int:
    from pyspark.sql import SparkSession
    spark: SparkSession = SparkSession.builder.getOrCreate()
    partitions = spark.sql(f'show partitions {table_name}').collect()
    return len(partitions)
