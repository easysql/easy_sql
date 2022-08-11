-- backend: clickhouse
-- config: easy_sql.udf_file_path=udf.py

-- target=log.test_udf
select translate('abcad', 'a', '') as translated_str
