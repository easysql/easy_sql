-- Preparation:
-- 1. start a local flink cluster: your/site-packages/path/pyflink/bin/start-cluster.sh
-- 2. ensure postgres started with configuration: `wal_level=logical` (in file /var/lib/postgresql/data/postgresql.conf)
-- 3. use remote mode to run flink application: configure `flink.cmd=-t remote` (already done below)
--
-- Verification:
-- 1. verify there are two rows in postgres table public.output_table
-- 2. verify there are two rows in postgres table public.output_table_agg
-- 3. insert data into sample.test and check if it shows up in public.output_table and aggregates correctly in public.output_table_agg
--
-- Cleanup:
-- 1. cancel applicaiton from flink dashboard (http://localhost:8081/)


-- backend: flink

-- config: easy_sql.flink_tables_file_path=test/sample_etl.flink_tables_file.json
-- config: easy_sql.etl_type=streaming

-- config: flink.cmd=-pyexec python3
-- config: flink.cmd=-pyclientexec python3
-- config: flink.cmd=-t remote
-- config: flink.pipeline.jars=test/flink/jars/flink-connector-jdbc-1.15.1.jar;test/flink/jars/flink-sql-connector-postgres-cdc-2.3.0.jar;test/flink/jars/postgresql-42.2.14.jar

-- inputs: db_pg.source_cdc
-- outputs: db_pg.target_1, db_pg.target_agg

-- prepare-sql: drop schema if exists sample cascade
-- prepare-sql: create schema sample
-- prepare-sql: create table sample.test as select 1 as id, '1' as val
-- prepare-sql: drop table if exists public.output_table
-- prepare-sql: create table public.output_table (id int4 PRIMARY KEY, val text)
-- prepare-sql: drop table if exists public.output_table_agg
-- prepare-sql: create table public.output_table_agg (val text PRIMARY KEY, count_val bigint)

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

-- target=output.db_pg.target_agg
select val, count(1) as count_val from result_view group by val

-- target=func.execute_streaming_inserts()
-- if there are multiple inserts and we call the function above, these inserts will be merged into one job and share streams
-- it takes the optimization method here: https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/sql/insert/

-- target=log.db_pg__target_1__count
select count(*) from db_pg.target_1
