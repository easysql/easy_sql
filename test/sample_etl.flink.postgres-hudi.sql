-- Preparation:
-- 1. download a hadoop release: wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
-- 2. set hadoop classpath: tar xf hadoop-3.3.4.tar.gz && export HADOOP_CLASSPATH=$($(pwd)/hadoop-3.3.4/bin/hadoop classpath)
-- 3. start a local flink cluster: your/site-packages/path/pyflink/bin/start-cluster.sh
-- 4. ensure postgres started with configuration: `wal_level=logical` (in file /var/lib/postgresql/data/postgresql.conf)
-- 5. use remote mode to run flink application: configure `flink.cmd=-t remote` (already done below)
--
-- Verification:
-- 1. verify there are two rows in hudi table /tmp/hudi-flink-test:
--     echo 'drop table if exists hudi_table;create table hudi_table using hudi location "/tmp/hudi-flink-test/db_hudi.db/target_hudi";select * from hudi_table;' | \
--     spark-sql --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.2 \
--         --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--         --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--         --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--         --conf 'spark.driver.extraJavaOptions="-Dderby.system.home=/tmp/spark-warehouse-metastore-hudi -Dderby.stream.error.file=/tmp/spark-warehouse-metastore-hudi.log"'
-- 2. insert data into sample.test and check if it shows up in the hudi table
--
-- Cleanup:
-- 1. cancel applicaiton from flink dashboard (http://localhost:8081/)


-- backend: flink

-- config: easy_sql.flink_tables_file_path=test/sample_etl.flink_tables_file.json
-- config: easy_sql.etl_type=streaming

-- config: flink.cmd=-pyexec python3
-- config: flink.cmd=-pyclientexec python3
-- config: flink.cmd=-t remote
-- config: flink.execution.checkpointing.interval=3s
-- config: flink.pipeline.jars=test/flink/jars/flink-sql-connector-postgres-cdc-2.3.0.jar;test/flink/jars/hudi-flink1.15-bundle-0.12.2.jar

-- inputs: db_pg.source_cdc
-- add db_pg.target_1 below to allow the prepare-sql command to execute against.
-- outputs: db_hudi.target_hudi, db_pg.target_1

-- prepare-sql: drop schema if exists sample cascade
-- prepare-sql: create schema sample
-- prepare-sql: create table sample.test as select 1 as id, '1' as val

-- target=variables
select
    'append'           as __save_mode__

-- target=temp.result_view
select
    2 as id,
    '2' as val
union all
select id, val from db_pg.source_cdc

-- target=output.db_hudi.target_hudi
select id, val from result_view

-- target=func.execute_streaming_inserts()
-- trigger execution of inserts manually, or it will be triggered at the end of the job and the query of db_hudi.target_hudi fails.

-- target=log.db_hudi__target_hudi
select * from db_hudi.target_hudi
