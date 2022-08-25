-- backend: flink
-- config: easy_sql.flink_tables_file_path=test/sample_etl.flink_tables_file.json

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
    cast(test_func(cast(${a} as BIGINT), 1) as string) as val
union all
select id, val from myhive.hive_table

-- target=output.myhive.hive_out_table
select id, val from result_view

-- target=log.sample_result
select * from result_view
