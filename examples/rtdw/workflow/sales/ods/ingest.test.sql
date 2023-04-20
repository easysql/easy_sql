-- backend: flink

-- config: easy_sql.flink_tables_file_path=ods.flink_tables.json
-- config: easy_sql.etl_type=batch

-- config: flink.cmd=-pyexec python3
-- config: flink.cmd=-pyclientexec python3
-- config: flink.cmd=-t remote
-- config: flink.execution.checkpointing.interval=3s
-- config: flink.pipeline.jars=../lib/flink/jars/flink-sql-connector-postgres-cdc-2.3.0.jar;../lib/flink/jars/hudi-flink1.15-bundle-0.12.2.jar

-- target=variables
select DATE_FORMAT(now(), 'yyyy-MM-dd') as TODAY;

-- target=func.exec_sql_in_source(${__step__}, db_pg, connector_jdbc)
-- prepare data to ingest
drop schema if exists ingest_test_sales cascade;
create schema ingest_test_sales;
create table ingest_test_sales.order (id int, product_id int, created_at timestamp, updated_at timestamp, primary key (id));
create table ingest_test_sales.product (id int, name text, category text, created_at timestamp, updated_at timestamp, primary key (id));
insert into ingest_test_sales.product values (1, 'p1', 'c1', '${TODAY} 00:00:00', '${TODAY} 00:00:00'), (2, 'p2', 'c2', '${TODAY} 00:00:01', '${TODAY} 00:00:01');
insert into ingest_test_sales.order values (1, 1, '${TODAY} 00:00:01', '${TODAY} 00:00:01'), (2, 1, '${TODAY} 00:00:01', '${TODAY} 00:00:01'), (3, 1, '${TODAY} 00:00:01', '${TODAY} 00:00:01');
insert into ingest_test_sales.order values (4, 2, '${TODAY} 00:00:01', '${TODAY} 00:00:01'), (5, 2, '${TODAY} 00:00:01', '${TODAY} 00:00:01');

-- target=func.test_run_etl(${__config__}, ingest.sql)

-- target=func.sleep(10)


-- target=check.ensure_product_data_ingested
select
    2 as expected
    , count(1) as actual
from ods_rt_sales.ingest_test_sales_product

-- target=check.ensure_order_data_ingested
select
    5 as expected
    , count(1) as actual
from ods_rt_sales.ingest_test_sales_order


-- target=func.exec_sql_in_source(${__step__}, db_pg, connector_jdbc)
-- prepare data to ingest
insert into ingest_test_sales.product values (3, 'p3', 'c3', '${TODAY} 00:00:00', '${TODAY} 00:00:00');
insert into ingest_test_sales.order values (6, 2, '${TODAY} 00:00:01', '${TODAY} 00:00:01');

-- target=func.sleep(5)


-- target=check.ensure_product_data_ingested
select
    3 as expected
    , count(1) as actual
from ods_rt_sales.ingest_test_sales_product
