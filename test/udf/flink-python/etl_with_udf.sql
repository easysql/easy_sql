-- backend: flink
-- config: easy_sql.udf_file_path=udf.py

-- target=log.test_udf
select test_func(1, 2) as stringset
