-- backend: flink
-- config: easy_sql.flink_tables_file_path=sample_etl.flink_tables_file_hive.yml
-- config: flink.cmd=-t local
-- config: flink.cmd=--parallelism 2
-- config: flink.python.fn-execution.bundle.size=1000
-- config: flink.python.client.executable=python
-- config: flink.jobmanager.memory.process.size=1024m
-- config: flink.taskmanager.memory.process.size=4096m

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
select id, val from myhiveCatalog.default.hive_table

-- target=output.myhiveCatalog_1.default.hive_out_table
select id, val from result_view

-- target=log.sample_result
select * from result_view
