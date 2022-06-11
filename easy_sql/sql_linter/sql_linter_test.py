import unittest

from easy_sql.sql_linter.sql_linter import SqlLinter


class SqlLinterTest(unittest.TestCase):

    def test_should_work_for_normal_sql(self):
        sql = '''select  a,
date,date2,
aa from sales_order
'''

        sql_linter = SqlLinter(sql,
                               include_rules=None,
                               exclude_rules=None)
        result = sql_linter.lint("bigquery",easysql=False)
        fixed = sql_linter.fix("bigquery",easysql=False)
        assert (len(result) == 7)
        print(fixed)
        expected = '''select a
    , date, date2
    , aa from ${temp_db}.sales_order
'''
        assert (expected == fixed)

    def test_should_work_for_exclude_rules(self):
        sql = '''select  a,
date,date2,
aa from sales_order
'''

        sql_linter = SqlLinter(sql,
                               include_rules=None,
                               exclude_rules=['BigQuery_L001'])
        result = sql_linter.lint("bigquery",easysql=False)
        fixed = sql_linter.fix("bigquery",easysql=False)
        assert (len(result) == 6)
        print(fixed)
        expected = '''select a
    , date, date2
    , aa from sales_order
'''
        assert (expected == fixed)




    def test_should_work_for_variables(self):
        sql = '''-- backend: bigquery
-- target=temp.feature_stage_0_out
select  a,
${date},${date2},
${${aa}} from sales_order
'''

        sql_linter = SqlLinter(sql,
                               include_rules=None,
                               exclude_rules=None)
        result = sql_linter.lint("bigquery")
        fixed = sql_linter.fix("bigquery")
        assert (len(result) == 7)
        expected = '''-- backend: bigquery
-- target=temp.feature_stage_0_out
select a
    , ${date}, ${date2}
    , ${${aa}} from ${temp_db}.sales_order
'''
        assert (expected == fixed)

    def test_should_work_when_have_three_quote(self):
        sql = '''-- backend: bigquery
-- target=temp.feature_stage_0_out
select  a,"""hh"""ab, "" ac from table
'''

        sql_linter = SqlLinter(sql,
                               include_rules=None,
                               exclude_rules=None)
        result = sql_linter.lint("bigquery")
        fixed = sql_linter.fix("bigquery")
        assert (len(result) == 5)
        expected = '''-- backend: bigquery
-- target=temp.feature_stage_0_out
select a, """hh""" as ab, "" as ac from ${temp_db}.table
'''
        assert (expected == fixed)

    def test_should_work_for_multiple_step(self):
        sql = """-- backend: bigquery
-- target=variables
select
    translate('${data_date}', '-', '')  as data_date_no_ds
    , true                              as __create_hive_table__
    
-- target=temp.model_data
select * from sales_model_demo_with_label where date='${data_date_no_ds}'


-- target=temp.feature_stage_0_out
select *
, ${data_date_no_ds}, a1, b1 from ${temp_db}.model_data
"""

        sql_linter = SqlLinter(sql,
                               include_rules=None,
                               exclude_rules=None)
        result = sql_linter.lint("bigquery")
        assert (len(result) == 10)
        fixed = sql_linter.fix("bigquery")
        expected = """-- backend: bigquery
-- target=variables
select
    translate('${data_date}', '-', '') as data_date_no_ds
    , true as __create_hive_table__

-- target=temp.model_data
select
    *
from ${temp_db}.sales_model_demo_with_label where date = '${data_date_no_ds}'

-- target=temp.feature_stage_0_out
select *
    , ${data_date_no_ds}, A1, B1 from ${temp_db}.Model_data
"""
        assert (fixed == expected)

    def test_should_work_when_have_template(self):
        # bigquery union must be union all
        sql = """-- backend: bigquery
-- target=template.dim_cols
product_name
, product_category

-- target=temp.dims
select @{dim_cols} from order_count
union all
select @{dim_cols} from sales_amount
"""
        sql_linter = SqlLinter(sql)
        result = sql_linter.lint("bigquery")
        assert (len(result) == 4)
        fixed = sql_linter.fix("bigquery")
        expected = """-- backend: bigquery
-- target=template.dim_cols
product_name
, product_category

-- target=temp.dims
select @{dim_cols} from ${temp_db}.Order_count
union all
select @{dim_cols} from ${temp_db}.Sales_amount
"""
        assert (expected == fixed)

    def test_should_work_when_given_diff_backend(self):
        # spark can only use union
        sql = """-- backend: spark
-- --------------------
-- 标签：产品销量
-- --------------------

-- config: spark.master=local[2]
-- config: spark.submit.deployMode=client
-- config: spark.executor.memory=1g
-- config: spark.executor.cores=1

-- inputs: dwd_sales.sales_fact_order_h, dwd_sales.sales_dim_product_h
-- outputs: dm_sales.product_label_amount
-- owner: all

-- target=template.dim_cols
product_name
, product_category

-- target=temp.dims
select @{dim_cols} from order_count
union 
select @{dim_cols} from sales_amount
"""
        sql_linter = SqlLinter(sql)
        result = sql_linter.lint("spark", True)
        assert (len(result) == 3)
        fixed = sql_linter.fix("spark")
        expected = """-- backend: spark
-- --------------------
-- 标签：产品销量
-- --------------------

-- config: spark.master=local[2]
-- config: spark.submit.deployMode=client
-- config: spark.executor.memory=1g
-- config: spark.executor.cores=1

-- inputs: dwd_sales.sales_fact_order_h, dwd_sales.sales_dim_product_h
-- outputs: dm_sales.product_label_amount
-- owner: all

-- target=template.dim_cols
product_name
, product_category

-- target=temp.dims
select @{dim_cols} from Order_count
union
select @{dim_cols} from Sales_amount
"""
        assert (fixed == expected)

    def test_should_work_when_have_functions(self):
        sql = """-- backend: bigquery

-- target=variables
select ${plus(1, 2)} as a
, flag as b
from data_table 

-- target=temp.dims
select * from order_count where value < ${a}

-- target=func.plus(1, 1)

"""
        sql_linter = SqlLinter(sql)
        result = sql_linter.lint("bigquery", True)
        print("after fix")
        print(len(result))
        fixed=sql_linter.fix("bigquery")
        print(fixed)
        assert (len(result) == 8)
        expected = """-- backend: bigquery

-- target=variables
select ${plus(1, 2)} as A
    , Flag as B
from ${temp_db}.Data_table

-- target=temp.dims
select * from ${temp_db}.order_count where value < ${a}

-- target=func.plus(1, 1)"""
        assert (expected == fixed)


if __name__ == '__main__':
    unittest.main()
