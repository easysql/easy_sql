import unittest

from easy_sql.sql_linter.sql_linter import SqlLinter


class SqlLinterTest(unittest.TestCase):

    def test_should_work_for_three_quote(self):
        sql = '''-- backend: bigquery
-- target=temp.feature_stage_0_out
select  a,
${date},${date2},"""a""" abs,
${${aa}}, ""a ,merge_cols_to_map('${all_cols_without_one_expr(check_result,table_name,fields,rule,scenario,description)}',
                      array(${all_cols_without_one_expr(check_result,table_name,fields,rule,scenario,description)}))
                                        result_detail
                                         from ${temp_db}.model_data
'''

        sql_linter = SqlLinter(sql,
                               include_rules=None,
                               exclude_rules=None)
        result = sql_linter.lint("bigquery")
        print("result")
        print(result)
        print(sql_linter.fix("bigquery"))


    def test_should_work_for_all_define_rule(self):
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
        assert (len(result) == 13)
        print("result")
        print(result)
        print(sql_linter.fix("bigquery"))

    def test_should_work_when_have_template(self):
        sql = """-- backend: bigquery
-- target=template.dim_cols
product_name
, product_category

-- target=temp.dims
select @{dim_cols} from order_count
union
select @{dim_cols} from sales_amount

-- target=template.join_conditions
dim.product_name <=> #{right_table}.product_name
and dim.product_category <=> #{right_table}.product_category

-- target=temp.joined_data
select
dim.product_name
, dim.product_category
, oc.order_count
, sa.sales_amount 
from dims dim 
left join order_count oc 
left join sales_amount sa on @{join_conditions(right_table=sa)} and  @{join_conditions(right_table=oc)} """
        sql_linter = SqlLinter(sql)
        result = sql_linter.lint("bigquery", True)
        # assert (len(result) == 15)
        print(sql_linter.fix("bigquery", False))

    def test_should_work_when_given_diff_backend(self):
        sql = """-- backend: spark
-- target=template.dim_cols
product_name
, product_category

-- target=temp.dims
select @{dim_cols} from order_count
union
select @{dim_cols} from sales_amount

-- target=template.join_conditions
dim.product_name <=> #{right_table}.product_name
and dim.product_category <=> #{right_table}.product_category

-- target=temp.joined_data
select
dim.product_name
, dim.product_category
, oc.order_count
, sa.sales_amount 
from dims dim 
left join order_count oc 
left join sales_amount sa on @{join_conditions(right_table=sa)} and  @{join_conditions(right_table=oc)} """
        sql_linter = SqlLinter(sql)
        result = sql_linter.lint("spark", True)
        print(len(result))
        print(sql_linter.fix("spark", False))

    def test_should_work_when_have_function(self):
        # todo: spark config header how to handle
        # use line number
        sql = """
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

-- target=variables
select ${plus(1, 2)} as a
, flag as b
from data_table 

-- target=temp.dims
select * from order_count where value < ${a}

-- target=func.plus(1, 1)

"""
        sql_linter = SqlLinter(sql, exclude_rules=['L009'])
        result = sql_linter.lint("bigquery", True)
        print("after fix")
        print(sql_linter.fix("bigquery"))
        # assert (len(result) == 8)

    def test_should_work_for_fix(self):
        sql = """-- backend: bigquery
-- target=variables
select
    translate('${data_date}', '-', '')  as data_date_no_ds
    , true                              as __create_hive_table__
    
-- target=temp.model_data
select * from sales_model_demo_with_label where date='${data_date_no_ds}'


-- target=temp.feature_stage_0_out
select * from ${temp_db}.model_data
"""
        sql_linter = SqlLinter(sql, exclude_rules=['L064', 'L034'])
        fixed_sql = sql_linter.fix("bigquery", True)
        print(fixed_sql)


if __name__ == '__main__':
    unittest.main()
