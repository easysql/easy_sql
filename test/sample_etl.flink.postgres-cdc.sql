-- Preparation:
-- 1. start a local flink cluster: your/site-packages/path/pyflink/bin/start-cluster.sh
-- 2. ensure postgres started with configuration: `wal_level=logical` (in file /var/lib/postgresql/data/postgresql.conf)
-- 3. use remote mode to run flink application: configure `flink.cmd=-t remote` (already done below)
--
-- Verification:
-- 1. verify there are two rows in postgres table public.output_table
-- 2. insert data into sample.test and check if it shows up in public.output_table
--
-- Cleanup:
-- 1. cancel applicaiton from flink dashboard (http://localhost:8081/)


-- backend: flink

-- config: easy_sql.flink_tables_file_path=test/sample_etl.flink_tables_file.yml
-- config: easy_sql.etl_type=streaming
-- config: easy_sql.prepare_sql_connector=connector_1

-- config: flink.cmd=-pyexec python3
-- config: flink.cmd=-pyclientexec python3
-- config: flink.cmd=-t remote
-- config: flink.pipeline.jars=test/flink/jars/flink-sql-connector-postgres-cdc-2.3.0.jar;test/flink/jars/hudi-flink1.15-bundle-0.12.2.jar;test/flink/jars/flink-sql-connector-hive-3.1.2_2.12-1.15.1.jar;test/flink/jars/postgresql-42.2.14.jar;test/flink/jars/flink-connector-jdbc-1.15.1.jar

-- inputs: db_pg.source_cdc
-- outputs: db_pg.target_1

-- prepare-sql: drop schema if exists sample cascade
-- prepare-sql: create schema sample
-- prepare-sql: create table sample.test as select 1 as id, '1' as val
-- prepare-sql: drop table if exists public.output_table
-- prepare-sql: create table public.output_table (id int4 PRIMARY KEY, val text)

-- target=variables
select
    'append'           as __save_mode__

-- target=variables
select 2 as a

-- target=log.a
select '${a}' as a

-- target=log.test_log
select 1 as some_log

-- target=check.should_equal
select 1 as actual, 1 as expected

-- target=temp.result_view
select
    ${a} as id,
    '2' as val
union all
select id, val from db_pg.source_cdc

-- target=output.db_pg.target_1
select id, val from result_view

-- target=log.db_pg__target_1
select * from db_pg.target_1
