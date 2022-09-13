-- backend: flink
-- config: flink.cmd=--jarfile udf.jar
-- config: easy_sql.scala_udf_initializer=your.company.udfs

-- target=log.test_udf
select test_func(1, 2) as sum_value
