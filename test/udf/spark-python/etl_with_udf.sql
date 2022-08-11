-- backend: spark
-- config: easy_sql.udf_file_path=udf.py

-- target=log.test_udf
select string_set(array("a", "a", "b")) as stringset
