# Variables

Easy SQL provides several special variables to help with ETL implementation.
These special variables are all starts with `__`.

Here are some description about what they are and how to use them.

## Variables to control data saving

- `__create_output_table__`: When true and the output table does not exist, will try to create output table automatically.
- `__partition__`: If specified, will save output data to the specified partition. There must be a partition column followed in the variable name.
As an example, if we defined variable `__partition__dt`, then dt will be the partition column and the value of the variable will be the partition value.
- `__save_mode__`: Value could be 'overwrite' or 'append'. If not specified, default to 'overwrite'. Will do append or overwrite when write data to table.

## Variables to control execution behaviour

- `__no_check__`: If true, will skip any `check` step defined by `-- target=check.xxx` for performance consideration.
- `__no_log__`: If true, will skip any `log` step defined by `-- target=log.xxx` for performance consideration.
- `__no_cache__`: If true, will create temporal table instead of cache table. This if for spark backend only. For the other backends, all the `cache` or `temp` table will be views.
- `__skip_all__`: If true, will skip execution of the following steps. Could be used when the partition of the input data does not exist.
- `__exception_handler__`: When specified, the value must be a function call.
The function call will be executed when there is an exception found during the execution of some step.
As an example, the value could be `some_exception_handler({__step__}, {var_a}, b)`. As we see, there could be variables referenced in the function call and the variable will be resolved when exception happens (at runtime, not definition time).

## Variables for function calling

- `__backend__`: An instance of [`Backend`]() class. Usually used to pass into functions.
- `__step__`: An instance of [`Step`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/step/index.html#easy_sql.sql_processor.step.Step) class. Usually used to pass into functions.
- `__context__`: An instance of [`ProcessorContext`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor/context/index.html#easy_sql.sql_processor.context.ProcessorContext) class. Usually used to pass into functions.
