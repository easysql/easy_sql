-- prepare-sql: drop database if exists sample cascade
-- prepare-sql: create database sample
-- prepare-sql: create table sample.order_table as select 1 as id, '1' as val
-- prepare-sql: create table sample.order_table_after_joined as select 1 as id, '1' as val

-- target=variables
select
    3 as c

-- target=log.i_would_like_to_log_something
select
    1 as a
    , 2 as b
    , ${c} as c

-- target=log.order_count
select
    count(1)
from sample.order_table

-- target=check.order_count_must_be_equal_after_joined_product
select
    (select count(1) from sample.order_table) as expected
    , (select count(1) from sample.order_table_after_joined) as actual

-- target=check.equal(${c}, 3)
