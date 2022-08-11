import os
from datetime import datetime

from easy_sql.sql_processor.backend.rdb import RdbBackend, SqlExpr, _exec_sql
from easy_sql.sql_processor.sql_processor import SqlProcessor

partition_col_converter = (
    lambda col: f"PARSE_DATE('%Y-%m', {col}) as {col}"
    if col in ["data_month", ":data_month"]
    else f"CAST({col} as DATE)"
)
partition_value_converter = (
    lambda col, value: datetime.strptime(value, "%Y-%m").date()
    if col == "data_month"
    else datetime.strptime(value, "%Y-%m-%d").date()
)
column_sql_type_converter = (
    lambda backend_type, col_name, col_type: "DATE" if col_name in ["di", "dt", "data_date", "data_month"] else None
)
partition_expr = (
    lambda backend_type, partition_col: f"DATE_TRUNC({partition_col}, MONTH)"
    if backend_type == "bigqiery" and partition_col == "data_month"
    else partition_col
)
sql_expr = SqlExpr(
    column_sql_type_converter=column_sql_type_converter,
    partition_col_converter=partition_col_converter,
    partition_value_converter=partition_value_converter,
    partition_expr=partition_expr,
)
backend = RdbBackend(
    "bigquery://",
    credentials=f"{os.environ.get('HOME', '/tmp')}/.bigquery/credential-prod.json",
    sql_expr=sql_expr,
    check_only=True,
)
sql = """-- backend: bigquery

-- target=variables
select
    translate('${data_date}', '-', '')  as data_date_no_ds
    , true                              as __create_hive_table__

-- target=temp.model_data
select * from sales_model_demo_with_label


-- target=temp.feature_stage_0_out
select * from ${temp_db}.model_data"""
sql_processor = SqlProcessor(
    backend,
    sql,
    [],
    variables={},
    report_hdfs_path=None,
    report_task_id="1",
    report_es_url=None,
    report_es_index_prefix=None,
    scala_udf_initializer=None,
)
check_results = sql_processor.lint()
for result in check_results:
    print("check result")
    print(result.__repr__())
