__all__ = ["read_kafka", "write_hudi", "read_hudi"]

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr, from_json, lit

spark = SparkSession.getActiveSession()


def read_kafka(result_table: str):
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "pgcdc")
        .option("startingOffsets", '{"pgcdc": {"0": 0}}')
        .option("failOnDataLoss", False)
        .option("maxOffsetsPerTrigger", 10)
        .option("maxTriggerDelay", "5s")
        .load()
    )
    df.createOrReplaceTempView(result_table)


def read_hudi(table: str):
    df = spark.read.format("hudi").load(f"/tmp/ingest_hudi/inventory.db/{table}")
    df.createOrReplaceTempView(table)


def _write_hudi_batch(batch_df: DataFrame, batch_id: int):
    batch = batch_df.collect()
    if not batch:
        return
    batch_df = spark.createDataFrame(batch, batch_df.schema)
    batch_df.persist()
    batch_df.show()

    for table in ["product", "user", "user_order"]:
        df = batch_df.filter(batch_df.table == table)
        if df.count() == 0:
            continue
        print("data:", df.selectExpr("data").rdd.map(lambda row: row[0]).collect())
        json_schema = spark.read.json(df.selectExpr("data").rdd.map(lambda row: row[0])).schema
        print("json schema:", json_schema)
        df = (
            df.select(
                from_json(df.data, json_schema).alias("json_data"), expr("is_delete as _hoodie_is_deleted"), expr("ts")
            )
            .selectExpr("json_data.*", "_hoodie_is_deleted", "ts")
            .withColumn(
                "create_time",
                expr(
                    "if(create_time = 0, cast(null as timestamp), "
                    "to_timestamp(from_unixtime(create_time / 1000000, 'yyyy-MM-dd HH:mm:ss')))"
                ),
            )
            .withColumn(
                "modify_time",
                expr(
                    "if(modify_time = 0, cast(null as timestamp), "
                    "to_timestamp(from_unixtime(modify_time / 1000000, 'yyyy-MM-dd HH:mm:ss')))"
                ),
            )
            .withColumn("_di", expr("from_unixtime(ts / 1000, 'yyyy-MM-dd')"))
        )
        df.show()

        # ## Schema change:
        # Add column:
        #   - no default value: automatically supported
        #   - with default value: emit a update sql in hudi manually
        # Remove column: emit a remove column sql in hudi manually
        # Rename column:
        #   - new data will be added to new column, null value set for the old column
        #   - need to do a migration in hudi manually
        # Change column type:
        #   - compatible: automatically supported
        #   - incompatible: fails, need to do a migration in hudi manually

        # ## CDC change:
        # Insert/Update: using upsert
        # Delete: supported by set _hoodie_is_deleted to true, set ts/_di to the correct value.
        #         ref: https://hudi.apache.org/docs/next/writing_data#deletes

        # if there are columns removed, fill it with None. # TODO: maybe raise exception?
        try:
            existing_schema = spark.read.format("hudi").load(f"/tmp/ingest_hudi/inventory.db/{table}").schema
            for f in set(existing_schema.fieldNames()) - set(df.schema.fieldNames()):
                df = df.withColumn(f, lit(None).astype(existing_schema[f].dataType))
        except Exception as e:
            if "java.io.FileNotFoundException" in str(e):
                print("table not exist yet, no need to try fill null for removed column")
            else:
                raise e

        (
            df.write.format("org.apache.hudi")
            .option("hoodie.table.name", table)
            .option("hoodie.datasource.write.recordkey.field", "id" if table != "product" else "pid")
            .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexKeyGenerator")
            .option("hoodie.datasource.write.partitionpath.field", "_di")
            .option("hoodie.datasource.write.precombine.field", "ts")
            .option("hoodie.datasource.write.operation", "upsert")
            .option("hoodie.datasource.write.payload.class", "org.apache.hudi.common.model.DefaultHoodieRecordPayload")
            .mode("append")
            .save(f"/tmp/ingest_hudi/inventory.db/{table}")
        )
        batch_df.unpersist()


def write_hudi(cdc_table: str = "", partition: int = 2):
    (
        spark.sql("select * from " + cdc_table)
        .selectExpr(
            "get_json_object(cast(value as string), '$.source.schema') as db",
            "get_json_object(cast(value as string), '$.source.table') as table",
            # Timestamp for when the change was made in the database.
            # ref: https://debezium.io/documentation/reference/1.6/connectors/postgresql.html#postgresql-update-events
            "get_json_object(cast(value as string), '$.source.ts_ms') as ts",
            "cast(get_json_object(cast(value as string), '$.after') as string) as after",
            "cast(get_json_object(cast(value as string), '$.before') as string) as before",
        )
        .withColumn("data", expr("coalesce(after, before)"))
        .withColumn("is_delete", expr("after is null"))
        .withColumn(
            "id",
            expr("get_json_object(coalesce(after, before), if(table == 'product', '$.pid', '$.id')) as id"),
        )
        .repartition(partition, "db", "table")
        .writeStream.option("checkpointLocation", "/tmp/spark-hudi-cdc-checkpoint")
        .foreachBatch(_write_hudi_batch)
        .start()
        .awaitTermination()
    )
