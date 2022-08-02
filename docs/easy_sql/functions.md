# Functions

While most of the ETL code is SQL, sometimes we would like to do things that are difficult to implement in SQL.

For example:

- Send out a http request to report status when some step of the ETL fails for some reasons.
- Check if a partition exists.
- Get the first partition value. 

These tasks could be easily implemented in python functions.

Easy SQL provides a way to call functions in ETL code. And there are a bunch of useful python functions provided by Easy SQL.
These functions are widely used in ETL development.

Below are a list of them for referencing.


### Functions for spark backend


#### Partition functions

- [`ensure_dwd_partition_exists(step, *args) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.ensure_dwd_partition_exists)
- [`ensure_partition_exists(step, *args) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.ensure_partition_exists)
- [`ensure_partition_or_first_partition_exists(step, *args) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.ensure_partition_or_first_partition_exists)
- [`get_first_partition(table_name: str) -> str`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_first_partition)
- [`get_first_partition_optional(table_name: str) -> Union[str, NoneType]`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_first_partition_optional)
- [`get_last_partition(table_name: str) -> str`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_last_partition)
- [`get_partition_col(table_name: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_partition_col)
- [`get_partition_cols(table_name: str) -> List[str]`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_spark/index.html#easy_sql.sql_processor.funcs_spark.PartitionFuncs.get_partition_cols)
- [`get_partition_or_first_partition(table_name: str, partition_value: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_partition_or_first_partition)
- [`has_partition_col(table_name: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.has_partition_col)
- [`is_first_partition(table_name: str, partition_value: str) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.is_first_partition)
- [`is_not_first_partition(table_name: str, partition_value: str) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.is_not_first_partition)
- [`partition_exists(table_name: str, partition_value: str) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.partition_exists)
- [`partition_not_exists(table_name: str, partition_value: str) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.partition_not_exists)
- [`previous_partition_exists(table_name: str, curr_partition_value_as_dt: str) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.previous_partition_exists)


#### Column functions

- [`all_cols_prefixed_with_exclusion_expr(table_name: str, prefix: str, *cols_to_exclude: str) -> str`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.ColumnFuncs.all_cols_prefixed_with_exclusion_expr)
- [`all_cols_with_exclusion_expr(table_name: str, *cols_to_exclude: str) -> str`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.ColumnFuncs.all_cols_with_exclusion_expr)
- [`all_cols_without_one_expr(table_name: str, *cols_to_exclude: str) -> str`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.ColumnFuncs.all_cols_without_one_expr)


#### Cache functions

- [`unpersist(table_name: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_spark/index.html#easy_sql.sql_processor.funcs_spark.CacheFuncs.unpersist)


#### Parallelism functions

- [`coalesce(table: str, partitions: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_spark/index.html#easy_sql.sql_processor.funcs_spark.ParallelismFuncs.coalesce)
- [`repartition(table: str, partitions: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_spark/index.html#easy_sql.sql_processor.funcs_spark.ParallelismFuncs.repartition)
- [`repartition_by_column(table: str, partitions: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_spark/index.html#easy_sql.sql_processor.funcs_spark.ParallelismFuncs.repartition_by_column)
- [`set_shuffle_partitions(partitions: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_spark/index.html#easy_sql.sql_processor.funcs_spark.ParallelismFuncs.set_shuffle_partitions)


#### IO functions

- [`rename_csv_output(spark_output_path: str, to_file: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_spark/index.html#easy_sql.sql_processor.funcs_spark.IOFuncs.rename_csv_output)
- [`update_json_local(context, vars: str, list_vars: str, json_attr: str, output_file: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_spark/index.html#easy_sql.sql_processor.funcs_spark.IOFuncs.update_json_local)
- [`write_csv(table: str, output_file: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_spark/index.html#easy_sql.sql_processor.funcs_spark.IOFuncs.write_csv)
- [`write_json_local(table: str, output_file: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_spark/index.html#easy_sql.sql_processor.funcs_spark.IOFuncs.write_json_local)


#### Alert functions

- [`alert(step, context, rule_name: str, pass_condition: str, alert_template: str, mentioned_users: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.AlertFunc.alert)
- [`alert_exception_handler(rule_name: str, mentioned_users: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.AlertFunc.alert_exception_handler)


#### Table functions

- [`check_not_null_column_in_table(step: easy_sql.sql_processor.step.Step, table_name: str, not_null_column: str, query: str = None) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.TableFuncs.check_not_null_column_in_table)
- [`ensure_no_null_data_in_table(step: easy_sql.sql_processor.step.Step, table_name: str, query: str = None) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.TableFuncs.ensure_no_null_data_in_table)


#### Model functions

- [`model_predict(model_save_path: str, table_name: str, feature_cols: str, id_col: str, output_ref_cols: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_spark/index.html#easy_sql.sql_processor.funcs_spark.ModelFuncs.model_predict)




### Functions for rdb backend (PostgreSQL Clickhouse BigQuery)


#### Partition functions

- [`ensure_dwd_partition_exists(step, *args) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.ensure_dwd_partition_exists)
- [`ensure_partition_exists(step, *args) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.ensure_partition_exists)
- [`ensure_partition_or_first_partition_exists(step, *args) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.ensure_partition_or_first_partition_exists)
- [`get_first_partition(table_name: str) -> str`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_first_partition)
- [`get_first_partition_optional(table_name: str) -> Union[str, NoneType]`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_first_partition_optional)
- [`get_last_partition(table_name: str) -> str`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_last_partition)
- [`get_partition_col(table_name: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_partition_col)
- [`get_partition_cols(table_name: str) -> List[str]`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_rdb/index.html#easy_sql.sql_processor.funcs_rdb.PartitionFuncs.get_partition_cols)
- [`get_partition_or_first_partition(table_name: str, partition_value: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_partition_or_first_partition)
- [`has_partition_col(table_name: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.has_partition_col)
- [`is_first_partition(table_name: str, partition_value: str) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.is_first_partition)
- [`is_not_first_partition(table_name: str, partition_value: str) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.is_not_first_partition)
- [`partition_exists(table_name: str, partition_value: str) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.partition_exists)
- [`partition_not_exists(table_name: str, partition_value: str) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.partition_not_exists)
- [`previous_partition_exists(table_name: str, curr_partition_value_as_dt: str) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.previous_partition_exists)


#### Column functions

- [`all_cols_prefixed_with_exclusion_expr(table_name: str, prefix: str, *cols_to_exclude: str) -> str`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.ColumnFuncs.all_cols_prefixed_with_exclusion_expr)
- [`all_cols_with_exclusion_expr(table_name: str, *cols_to_exclude: str) -> str`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.ColumnFuncs.all_cols_with_exclusion_expr)
- [`all_cols_without_one_expr(table_name: str, *cols_to_exclude: str) -> str`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.ColumnFuncs.all_cols_without_one_expr)


#### Alert functions

- [`alert(step, context, rule_name: str, pass_condition: str, alert_template: str, mentioned_users: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.AlertFunc.alert)
- [`alert_exception_handler(rule_name: str, mentioned_users: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.AlertFunc.alert_exception_handler)


#### Table functions

- [`check_not_null_column_in_table(step: easy_sql.sql_processor.step.Step, table_name: str, not_null_column: str, query: str = None) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.TableFuncs.check_not_null_column_in_table)
- [`ensure_no_null_data_in_table(step: easy_sql.sql_processor.step.Step, table_name: str, query: str = None) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.TableFuncs.ensure_no_null_data_in_table)


#### Model functions

- [`bq_model_predict_with_tmp_spark(model_save_path: str, input_table_name: str, output_table_name: str, feature_cols: str, id_col: str, output_ref_cols: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_rdb/index.html#easy_sql.sql_processor.funcs_rdb.ModelFuncs.bq_model_predict_with_tmp_spark)
- [`model_predict_with_local_spark(model_save_path: str, input_table_name: str, output_table_name: str, feature_cols: str, id_col: str, output_ref_cols: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_rdb/index.html#easy_sql.sql_processor.funcs_rdb.ModelFuncs.model_predict_with_local_spark)


