-- backend: spark
-- config: spark.jars=udf.jar
-- config: easy_sql.scala_udf_initializer=your.company.udfs

-- target=log.test_udf
select string_set(array("a", "a", "b")) as stringset
