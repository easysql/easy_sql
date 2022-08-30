-- backend: flink
-- config: easy_sql.flink_tables_file_path=test/sample_etl.flink_tables_file.json

-- prepare-sql: USE CATALOG myhiveCatalog
-- prepare-sql: DROP DATABASE IF EXISTS myhive CASCADE
-- prepare-sql: CREATE DATABASE myhive
-- prepare-sql: create table myhive.hive_table as select 1 as id, '1' as val
-- prepare-sql: drop table if exists public.out_put_table
-- prepare-sql: create table public.out_put_table (id int4 PRIMARY KEY, val text)

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
select id, val from myhiveCatalog.myhive.hive_table

-- target=output.myhiveCatalog.myhive.hive_out_table
select id, val from result_view

-- target=log.sample_result
select * from result_view
