-- config: easy_sql.spark_submit=spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.2
-- config: easy_sql.func_file_path=ingest_hudi_funcs.py
-- config: easy_sql.etl_type=streaming

-- config: spark.serializer=org.apache.spark.serializer.KryoSerializer
-- config: spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
-- config: spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog

-- target=func.read_kafka(pgcdc)
-- target=func.write_hudi(pgcdc)
