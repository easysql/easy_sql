<!-- Notes: this doc need to be updated manually with script: update_doc.py -->

# Functions

While most of the ETL code is SQL, sometimes we would like to do things that are difficult to implement in SQL.

For example:

- Send out a http request to report status when some step of the ETL fails for some reasons.
- Check if a partition exists.
- Get the first partition value.

These tasks could be easily implemented in python functions.

Easy SQL provides a way to call functions in ETL code. And there are a bunch of useful python functions provided by Easy SQL.
These functions are widely used in ETL development.

## Use functions in ETL

To use the functions provided by Easy SQL, nothing need to be done. Just call it in a `func` `check` or `variables` step.

If you would like to implement a new function and use it in ETL, you need to register it before the execution of the ETL.

Let's have a look at a simple example of how to create and register a new function and use it in ETL.

### Implement your customized function

First of all, you need to implement your function in a python file.
Let's assume we're using spark backend, and the function we'd like to implement is a function to count the partitions of some table.

We could create a file named `customized_func.py`, with content:

```python
def count_partitions(table_name: str) -> int:
    from pyspark.sql import SparkSession
    spark: SparkSession = SparkSession.builder.getOrCreate()
    partitions = spark.sql(f'show partitions {table_name}').collect()
    return len(partitions)
```

### Use function in ETL

If we use the command line interface to run the ETL, we need to configure it with a property named `easy_sql.func_file_path` in your ETL file.
An example is as below:

```sql
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
```

If we save the above content to a file named `etl_with_customized_func.sql`, and run it with command:

```bash
bash -c "$(python3 -m easy_sql.data_process -f etl_with_customized_func.sql -p)"
```

We'll see the output like below:

```
===================== REPORT FOR step-1 ==================
config: StepConfig(target=action.define_table, condition=None, line_no=4)
sql: create table some_table partitioned by (pt) as
select * from (
    select 1 as a, 2 as b, 1 as pt
    union
    select 1 as a, 2 as b, 2 as pt
) t
status: SUCCEEDED
start time: 2022-08-03 10:33:33, end time: 2022-08-03 10:33:43, execution time: 9.839801s - 96.62%
messages:


===================== REPORT FOR step-2 ==================
config: StepConfig(target=log.partition_count, condition=None, line_no=12)
sql: select 2 as partition_count
status: SUCCEEDED
start time: 2022-08-03 10:33:43, end time: 2022-08-03 10:33:43, execution time: 0.254982s - 2.50%
messages:
partition_count=2
```

**Notes:**

- The command line interface will import and register all the functions in the python file. So we suggest defining all of your functions in a `__all__` variable.
- The function file path in configuration `-- config: easy_sql.func_file_path=some_function.py` is either an absolute path or a path relative to current working directory.

### Register functions programmatically

Easy SQL also provides a way to register functions programmatically.
The api to register functions is [`register_funcs_from_pyfile`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/sql_processor/index.html#easy_sql.sql_processor.sql_processor.SqlProcessor.register_funcs_from_pyfile).

The example code to register functions is:

```python
from pyspark.sql import SparkSession

from easy_sql.sql_processor import SqlProcessor
from easy_sql.sql_processor.backend import SparkBackend

if __name__ == '__main__':
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    backend = SparkBackend(spark)
    sql = '''
-- target=log.some_log
select 1 as a
    '''
    sql_processor = SqlProcessor(backend, sql)
    sql_processor.register_funcs_from_pyfile('some_function.py')
    sql_processor.run()
```

**Note:**

- The python file `some_function.py` must be able to be found in module search path (Add the directory path of the file to `PYTHONPATH` environment variable).

## Builtin and operator functions

By default, Easy SQL imported all the functions in python `builtin` module and `operator` module.

So we can use any functions listed here:

- [`builtin` functions](https://docs.python.org/3/library/functions.html#built-in-funcs)
- [`operator` functions](https://docs.python.org/3/library/operator.html#module-operator)

For convenience, there are several utility functions implemented as well:

- `equal(a: any, b: any)`
- `is_greater_or_equal(a: str|int|float, b: str|int|float)`
- `equal_ignore_case(a: str, b: str)`

## Functions implemented in Easy SQL

Below are a list of them for referencing.


### Functions for spark backend


#### Partition functions

- [`ensure_dwd_partition_exists(step: Step, *args) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.ensure_dwd_partition_exists)
- [`ensure_partition_exists(step: Step, *args) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.ensure_partition_exists)
- [`ensure_partition_or_first_partition_exists(step: Step, *args) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.ensure_partition_or_first_partition_exists)
- [`get_first_partition(table_name: str) -> str`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_first_partition)
- [`get_first_partition_optional(table_name: str) -> Optional[str]`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_first_partition_optional)
- [`get_last_partition(table_name: str) -> str`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_last_partition)
- [`get_partition_col(table_name: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_partition_col)
- [`get_partition_cols(table_name: str) -> List[str]`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_spark/index.html#easy_sql.sql_processor.funcs_spark.PartitionFuncs.get_partition_cols)
- [`get_partition_or_first_partition(table_name: str, partition_value: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_partition_or_first_partition)
- [`get_partition_values_as_joined_str(table_name: str) -> str`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_partition_values_as_joined_str)
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

- [`move_file(source_file: str, target_file: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.IOFuncs.move_file)
- [`rename_csv_output(spark_output_path: str, to_file: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_spark/index.html#easy_sql.sql_processor.funcs_spark.IOFuncs.rename_csv_output)
- [`update_json_local(context: ProcessorContext, vars: str, list_vars: str, json_attr: str, output_file: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_spark/index.html#easy_sql.sql_processor.funcs_spark.IOFuncs.update_json_local)
- [`write_csv(table: str, output_file: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_spark/index.html#easy_sql.sql_processor.funcs_spark.IOFuncs.write_csv)
- [`write_json_local(table: str, output_file: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_spark/index.html#easy_sql.sql_processor.funcs_spark.IOFuncs.write_json_local)


#### Alert functions

- [`alert(step: Step, context: ProcessorContext, rule_name: str, pass_condition: str, alert_template: str, mentioned_users: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.AlertFunc.alert)
- [`alert_exception_handler(rule_name: str, mentioned_users: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.AlertFunc.alert_exception_handler)
- [`alert_with_backend(backend: Backend, step: Step, context: ProcessorContext, rule_name: str, pass_condition: str, alert_template: str, mentioned_users: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.AlertFunc.alert_with_backend)


#### Table functions

- [`check_not_null_column_in_table(step: Step, table_name: str, not_null_column: str, query: Optional[str] = None) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.TableFuncs.check_not_null_column_in_table)
- [`ensure_no_null_data_in_table(step: Step, table_name: str, query: Optional[str] = None) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.TableFuncs.ensure_no_null_data_in_table)


#### Model functions

- [`model_predict(model_save_path: str, table_name: str, feature_cols: str, id_col: str, output_ref_cols: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_spark/index.html#easy_sql.sql_processor.funcs_spark.ModelFuncs.model_predict)


#### Analytics functions

- [`data_profiling_report(table: str, query: str, output_folder: str, max_count=50000)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.AnalyticsFuncs.data_profiling_report)




### Functions for rdb backend (PostgreSQL Clickhouse BigQuery)


#### Partition functions

- [`ensure_dwd_partition_exists(step: Step, *args) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.ensure_dwd_partition_exists)
- [`ensure_partition_exists(step: Step, *args) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.ensure_partition_exists)
- [`ensure_partition_or_first_partition_exists(step: Step, *args) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.ensure_partition_or_first_partition_exists)
- [`get_first_partition(table_name: str) -> str`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_first_partition)
- [`get_first_partition_optional(table_name: str) -> Optional[str]`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_first_partition_optional)
- [`get_last_partition(table_name: str) -> str`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_last_partition)
- [`get_partition_col(table_name: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_partition_col)
- [`get_partition_cols(table_name: str) -> List[str]`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_rdb/index.html#easy_sql.sql_processor.funcs_rdb.PartitionFuncs.get_partition_cols)
- [`get_partition_or_first_partition(table_name: str, partition_value: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_partition_or_first_partition)
- [`get_partition_values_as_joined_str(table_name: str) -> str`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.PartitionFuncs.get_partition_values_as_joined_str)
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

- [`alert(step: Step, context: ProcessorContext, rule_name: str, pass_condition: str, alert_template: str, mentioned_users: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.AlertFunc.alert)
- [`alert_exception_handler(rule_name: str, mentioned_users: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.AlertFunc.alert_exception_handler)
- [`alert_with_backend(backend: Backend, step: Step, context: ProcessorContext, rule_name: str, pass_condition: str, alert_template: str, mentioned_users: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.AlertFunc.alert_with_backend)


#### Table functions

- [`check_not_null_column_in_table(step: Step, table_name: str, not_null_column: str, query: Optional[str] = None) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.TableFuncs.check_not_null_column_in_table)
- [`ensure_no_null_data_in_table(step: Step, table_name: str, query: Optional[str] = None) -> bool`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.TableFuncs.ensure_no_null_data_in_table)


#### Model functions

- [`bq_model_predict_with_local_spark(model_save_path: str, input_table_name: str, output_table_name: str, feature_cols: str, id_col: str, output_ref_cols: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_rdb/index.html#easy_sql.sql_processor.funcs_rdb.ModelFuncs.bq_model_predict_with_local_spark)
- [`model_predict_with_local_spark(model_save_path: str, input_table_name: str, output_table_name: str, feature_cols: str, id_col: str, output_ref_cols: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_rdb/index.html#easy_sql.sql_processor.funcs_rdb.ModelFuncs.model_predict_with_local_spark)


#### IO functions

- [`move_file(source_file: str, target_file: str)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.IOFuncs.move_file)


#### Analytics functions

- [`data_profiling_report(table: str, query: str, output_folder: str, max_count=50000)`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/funcs_common/index.html#easy_sql.sql_processor.funcs_common.AnalyticsFuncs.data_profiling_report)
