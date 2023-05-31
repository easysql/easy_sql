-- backend: flink

-- config: easy_sql.flink_tables_file_path=test/sample_etl.flink_tables_file.yml

-- config: flink.cmd=-pyexec python3
-- config: flink.cmd=-pyclientexec python3
-- config: flink.cmd=-t local
-- config: flink.pipeline.jars=test/flink/jars/flink-connector-jdbc-1.15.1.jar;test/flink/jars/postgresql-42.2.14.jar

-- inputs: db_pg.source_1, db_pg.target_1
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
select id, val from db_pg.source_1

-- target=output.db_pg.target_1
select id, val from result_view

-- target=log.sample_result
select * from result_view
