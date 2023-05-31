-- Preparation: refer sample_etl.flink.postgres-hudi.sql
--
-- Verification:
-- 1. verify there are two rows in hudi table:
--     - start sql client: /usr/local/lib/python3.8/site-packages/pyflink/bin/sql-client.sh embedded -j test/flink/jars/hudi-flink1.15-bundle-0.12.2.jar shell
--     - emit sql: create table hudi_agg (val varchar NOT NULL PRIMARY KEY NOT ENFORCED, val_count bigint) WITH (
--                    'connector' = 'hudi' , 'path' = '/tmp/hudi-flink-test/db_hudi.db/target_hudi_agg' , 'table.type' = 'MERGE_ON_READ' , 'changelog.enabled' = 'True' , 'compaction.async.enabled' = 'False'
--                );
--     - emit sql: select * from hudi_agg;
-- 2. insert data into sample.test and check if it aggregates correctly in hudi table
--
-- Cleanup:
-- 1. cancel applicaiton from flink dashboard (http://localhost:8081/)


-- backend: flink

-- config: easy_sql.flink_tables_file_path=test/sample_etl.flink_tables_file.yml
-- config: easy_sql.etl_type=streaming

-- config: flink.cmd=-pyexec python3
-- config: flink.cmd=-pyclientexec python3
-- config: flink.cmd=-t remote
-- config: flink.execution.checkpointing.interval=3s
-- config: flink.pipeline.jars=test/flink/jars/flink-sql-connector-postgres-cdc-2.3.0.jar;test/flink/jars/hudi-flink1.15-bundle-0.12.2.jar

-- prepare-sql: drop schema if exists sample cascade
-- prepare-sql: create schema sample
-- prepare-sql: create table sample.test as select 1 as id, '1' as val

-- inputs: db_pg.source_cdc
-- add db_pg.target_1 below to allow the prepare-sql command to execute against.
-- outputs: db_hudi.target_hudi_agg, db_pg.target_1

-- target=variables
select
    'append'           as __save_mode__

-- target=temp.result_view
select
    2 as id
    ,'2' as val
union all
select id, val from db_pg.source_cdc

-- target=output.db_hudi.target_hudi_agg
select val, count(*) as val_count from result_view group by val
