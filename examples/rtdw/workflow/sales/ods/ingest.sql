-- backend: flink

-- config: easy_sql.flink_tables_file_path=ods.flink_tables.json
-- config: easy_sql.func_file_path=ingest_funcs.py
-- config: easy_sql.etl_type=streaming

-- config: flink.cmd=-pyexec python3
-- config: flink.cmd=-pyclientexec python3
-- config: flink.cmd=-t remote
-- config: flink.execution.checkpointing.interval=3s
-- config: flink.pipeline.jars=../lib/flink/jars/flink-sql-connector-postgres-cdc-2.3.0.jar;../lib/flink/jars/hudi-flink1.15-bundle-0.12.2.jar;sales/ods/easysql-example-ingest.jar;sales/ods/jars/flink-connector-jdbc-1.15.2.jar;sales/ods/jars/postgresql-42.2.14.jar
-- config: flink.pipeline.name=sales.ingest

-- target=variables
select
    'append'           as __save_mode__
    , 'inventory.user,inventory.product,inventory.user_order'          as tables_

-- target=func.ingest_cdc_pg(${__backend__}, db_pg, connector_cdc, ${tables_}, sales)
