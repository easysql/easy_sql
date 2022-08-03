-- backend: spark
-- config: easy_sql.func_file_path=customized_func.py

-- target=action.define_table
create table some_table partitioned by (pt) as
select * from (
    select 1 as a, 2 as b, 1 as pt
    union
    select 1 as a, 2 as b, 2 as pt
) t

-- target=log.partition_count
select ${count_partitions(some_table)} as partition_count