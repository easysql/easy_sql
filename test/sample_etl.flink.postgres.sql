-- backend: flink
-- config: easy_sql.flink_source_file=test/sample_etl.flink_source_config.json
-- inputs: db_1.source_1, db_1.target_1
-- outputs: db_1.target_1

-- prepare-sql: drop schema if exists sample cascade
-- prepare-sql: create schema sample
-- prepare-sql: create table sample.test as select 1 as id, '1' as val
-- prepare-sql: drop table if exists public.out_put_table
-- prepare-sql: create table public.out_put_table (id int4 PRIMARY KEY, val text)

-- target=variables
select 
    'append'           as __save_mode__

-- target=variables
select 1 as a

-- target=log.a
select '${a}' as a

-- target=log.test_log
select 1 as some_log

-- target=check.should_equal
select 1 as actual, 1 as expected

-- target=temp.result_view
select
    ${a} as id,
    cast(test_func(cast(${a} as BIGINT), 1) as string) as val
union all
select id, val from db_1.source_1

-- target=output.db_1.target_1
select id, val from result_view

-- target=log.sample_result
select * from result_view
