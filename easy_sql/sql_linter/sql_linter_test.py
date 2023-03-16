import unittest

from easy_sql.sql_linter.sql_linter import SqlLinter


class SqlLinterTest(unittest.TestCase):
    def test_should_work_for_normal_sql(self):
        sql = """select  a,
date,date2,
aa from sales_order
"""

        sql_linter = SqlLinter(sql, include_rules=None, exclude_rules=None)
        result = sql_linter.lint("bigquery", easysql=False)
        fixed = sql_linter.fix("bigquery", easy_sql=False)
        self.assertEqual(len(result), 7)
        print(fixed)
        expected = """select a
    , date, date2
    , aa from ${temp_db}.sales_order
"""
        self.assertEqual(expected, fixed)

    def test_should_work_for_exclude_rules(self):
        sql = """select  a,
date,date2,
aa from sales_order
"""

        sql_linter = SqlLinter(sql, include_rules=None, exclude_rules=["BigQuery_L001"])
        result = sql_linter.lint("bigquery", easysql=False)
        fixed = sql_linter.fix("bigquery", easy_sql=False)
        self.assertEqual(len(result), 6)
        print(fixed)
        expected = """select a
    , date, date2
    , aa from sales_order
"""
        self.assertEqual(expected, fixed)

    def test_should_work_for_variables(self):
        sql = """-- backend: bigquery
-- target=temp.feature_stage_0_out
select  a,
${date},${date2},
${${aa}} from sales_order
"""

        sql_linter = SqlLinter(sql, include_rules=None, exclude_rules=None)
        result = sql_linter.lint("bigquery")
        fixed = sql_linter.fix("bigquery")
        self.assertEqual(len(result), 7)
        expected = """-- backend: bigquery
-- target=temp.feature_stage_0_out
select a
    , ${date}, ${date2}
    , ${${aa}} from ${temp_db}.sales_order
"""
        self.assertEqual(expected, fixed)

    def test_should_work_when_have_three_quote(self):
        sql = '''-- backend: bigquery
-- target=temp.feature_stage_0_out
select  a,"""hh"""ab, "" ac from table
'''

        sql_linter = SqlLinter(sql, include_rules=None, exclude_rules=None)
        result = sql_linter.lint("bigquery")
        fixed = sql_linter.fix("bigquery")
        self.assertEqual(len(result), 5)
        expected = '''-- backend: bigquery
-- target=temp.feature_stage_0_out
select a, """hh""" as ab, "" as ac from ${temp_db}.table
'''
        self.assertEqual(expected, fixed)

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

        sql_linter = SqlLinter(sql, include_rules=None, exclude_rules=None)
        result = sql_linter.lint("bigquery")
        self.assertEqual(len(result), 8)
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
    , ${data_date_no_ds}, a1, b1 from ${temp_db}.model_data
"""
        self.assertEqual(fixed, expected)

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

-- target=temp.result
@{transited_with_fk_inc_template(
    source_table_name=transited, source_table_biz_key=product_id, result_col_name=product_id_key,
    fk_table_name=dwd_sales.sales_dim_product_h, pk=product_key, fk_table_biz_key=id,
    update_time_col_name=update_time, partition_col_name=di)}
"""
        sql_linter = SqlLinter(sql, exclude_rules=["L025"])
        result = sql_linter.lint("bigquery")
        self.assertEqual(len(result), 2)
        fixed = sql_linter.fix("bigquery")
        print(fixed)
        expected = """-- backend: bigquery
-- target=template.dim_cols
product_name
, product_category

-- target=temp.dims
select @{dim_cols} from ${temp_db}.order_count
union all
select @{dim_cols} from ${temp_db}.sales_amount

-- target=temp.result
@{transited_with_fk_inc_template(
    source_table_name=transited, source_table_biz_key=product_id, result_col_name=product_id_key,
    fk_table_name=dwd_sales.sales_dim_product_h, pk=product_key, fk_table_biz_key=id,
    update_time_col_name=update_time, partition_col_name=di)}"""
        self.assertEqual(expected, fixed)

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
        self.assertEqual(len(result), 0)
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
select @{dim_cols} from order_count
union
select @{dim_cols} from sales_amount
"""
        self.assertEqual(fixed, expected)

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
        fixed = sql_linter.fix("bigquery")
        print(fixed)
        self.assertEqual(len(result), 3)
        expected = """-- backend: bigquery

-- target=variables
select ${plus(1, 2)} as a
    , flag as b
from ${temp_db}.data_table

-- target=temp.dims
select * from ${temp_db}.order_count where value < ${a}

-- target=func.plus(1, 1)"""
        self.assertEqual(expected, fixed)


if __name__ == "__main__":
    unittest.main()
