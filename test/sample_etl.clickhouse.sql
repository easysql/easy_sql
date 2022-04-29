-- backend: clickhouse
-- prepare-sql: drop database if exists sample
-- prepare-sql: create database sample
-- prepare-sql: create table sample.test engine MergeTree() order by tuple() as select 1 as id, '1' as val

-- target=variables
select 1 as __create_output_table__

-- target=variables
select 1 as a

-- target=log.a
select '${a}' as a

-- target=log.test_log
select 1 as some_log

-- target=check.should_equal
select 1 as actual, 1 as expected

-- target=temp.result
select
    ${a} as id, cast(${a} + 1 as text) as val
union all
select id, val from sample.test

-- target=output.sample.result
select * from result

-- target=log.sample_result
select * from result
