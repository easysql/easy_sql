# Syntax

Easy SQL defines a few customized syntax on top of SQL to add imperative characteristics.

## Language features in Easy SQL

For Easy SQL, guided by the design principles, there are a few simple language features added to support these imperative characteristics.

Below is a list of these features:

- An imperative structure of ETL code.
- Variables which could be defined and modified any time.
- A way to call external functions.
- A way to control whether a step should be executed.
- Templates that could be reused in the same ETL file.
- Include command that could be used to reuse code at file level.
- Debugging support: logging and assertion that could be used for debugging.
- A debugger interface.
- Other features：write data to tables; list variables; SQL actions.

Let's have a look at the first four features.

## Syntax in Easy SQL

### The imperative structure

The most obvious characteristics of imperative programming is that code will be executed line by line (or piece by piece).
And the declarative way (standard SQL) suggests the opposite, which says, all logic should be defined first and then be executed in one final action.

The major task of designing the imperative structure is to introduce a way to execute SQL step by step.
If we look at Spark DataFrame API, we could find that it works in an imperative way.
For example, we can assign a DataFrame to some variable, then do something about the variable, then transform the variable and assign it to another variable.

In Easy SQL, a simple syntax is introduced as SQL comment, which is `target=` and `-- target=SOME_TARGET` in Easy SQL.

There are a few built-in types of targets, which are:

- variables
- temp
- cache
- broadcast
- func
- log
- check
- output
- template
- list_variables
- action

When used in Easy SQL ETL, it looks like below:

```sql
-- target=variables
select 1 as a, '2' as b

-- target=cache.table_a
select
    *
from some_db.table_a

-- target=temp.table_b
select
    *
from some_db.table_b

-- target=broadcast.table_c
select
    *
from some_db.table_c

-- target=output.some_db.some_table
select
    *
from table_a a
    left join table_b b on a.id=b.id
    left join table_c c on a.id=c.id
```

There is a SQL query under every `target` statement.
It means the result of the SQL query is saved to the specified target.
Each `target` could be viewed as a step and will be executed one by one.

The syntax may seem obvious to most of you. If it is so, it means the design goal is achieved.

Let's spend some time explaining what it does in the simple ETL above.
If you believe you got it already, please skip it.

1. The first `target` statement means the SQL query result, `a=1 and b='2'` in this case, will be saved to the variables target. After this step, the variables could be used.
2. The second `target` statement means that the query from table_a will be saved to a cached table named 'table_a'. Cache table is a concept borrowed from Spark and it means the query result will be cached and could be reused from the following steps to improve performance.
3. The third `target` statement means that the query from table_b will be saved to a temporary table named 'table_b'. And 'table_b' could be used in the following steps.
4. The forth `target` statement means that the query from table_c will be saved to a broadcasted table named 'table_c'. Broadcast table is also a concept borrowed from Spark and it means the table will be broadcasted to every node to improve performance. And the table 'table_c' could be used in the following steps too.
5. The fifth `target` statement means that the joined query result of the above 3 tables will be saved to an output table named 'some_table' in 'some_db'.

### Variables

Variables could be defined and modified at any step. The syntax is as the case above.
If we'd like to modify the value of it, we can just add another `variables` target and write a query with the result of changed 'a' and 'b'.
A simple example is as below:

```sql
-- target=variables
select 1 as a, 2 as b
-- target=variables
select 2 as a, 1 as b
```

After the two steps, the value of a will be 2, and it will be 1 for the value of b.

Variables could be referenced anywhere in the following steps with syntax '${VAR_NAME}'.

There is a simple example below:

```sql
-- target=variables
select 1 as a, '2' as b
-- target=variables
select
    ${a} as a
    , ${b} as b
    , 1${a} as a1
    , ${a} + ${b} as ab
```

When Easy SQL engine reaches the second step, it will do a variable lookup and simply replace the reference with the real value.
It will replace it with the string value of the variable and converts types when required.

The above example will result in variables: `a=1, b=2, a1=11, ab=3`.

Besides the user-defined variables, there are a few useful system-defined variables.
When we need to implement some complicated functions, we can use them. A list of these variables could be found [here](variables.md).

Other things to note about variables:

- Variable name must be composed of chars '0-9a-zA-Z_'.
- Variable name is case-insensitive.

### Temporary tables

Another common case is to save a query to a temporary table for later query. We have already seen a concrete example above.

It works simply as what you expected.

- The query result will be saved to a temporary table if the target is 'temp'.
- The query result will be saved to a cached temporary table if the target is 'cache'.
- The query result will be saved to a broadcasted temporary table if the target is 'broadcast'.

Speaking of implementation, if the backend is Spark, 'temp' 'cache' and 'broadcast' behave the same as that in Spark,
and with a global temporary table created or replaced with the specified name.
For the other backends in which there is no support of caching and broadcasting of temporary tables,
Easy SQL just create views with the specified name in a temporary default database.

Since there is no actual loading of data for temporary tables, to define a temporary table is a very light-weight operation.
You can create as many temporary tables as you wish.

There are a few things to note when creating temporary tables for different backends.

- For Spark backend, the name of the temporary table can be reused, but it cannot be reused for the other backends since we cannot create two database views with the same name in the same default database.
- For BigQuery backend, we have to specify names like `${temp_db}.SOME_TEMP_TABLE_NAME` when query the created temporary table. You guessed it, the 'temp_db' is a pre-defined variable provided by Easy SQL engine. This limitation is introduced by BigQuery since there is no such concept of a default database (named dataset in BigQuery).

### Function calls

Function calls is another feature introduced by Easy SQL. It is used to expand the ability of SQL, so that we could do anything in SQL.

Function is defined in Python code and registered before the execution of the ETL.
The syntax to call a function is very intuitive if you have experience of some other programming languages.

Below is an example of function calls.

```sql
-- target=func.plus(1, 1)

-- target=func.do_some_thing()

-- target=variables
select ${plus(2, 2)} as a

-- target=variables
select ${plus(${a}, 2)} as b
```

From the ETL code above, we can find a few things about function calls:

- Function calls could be used as a 'func' target.
- The result of function calls could be used as a variable.
- Parameters of function calls could be variables.

Besides these, there are a few other things to note:

- One function call expression must be in one line of code.
- When functions are called, all non-variable parameters are passed as strings even if it looks like an integer. In the function implementation, we need to convert types from string to its real type.
- There should be no chars from any of `,()` in literal parameters. If there is, we need to define a variable before the function call and pass in the variable as a parameter.
- Any user-defined variable will be converted to string and passed to functions as string value. We may need to convert types in the function implementation.
- All functions in the Python `builtin` module and `operators` module are automatically registered, so we can use a lot of Python functions without providing an implementation.

Before execution of the above ETL, we need to define the functions referenced in the ETL. Followed by the above rules, an example of the function implementations could be:

```python
def plus(a: str, b: str) -> int:
    return int(a) + int(b)

def do_some_thing():
    print('do things...')
```

And then after the execution of the ETL, the value of the variables will be: `a=4, b=6`.

For the usage of function in Easy SQL and a list of useful built-in functions in Easy SQL. Please view [functions](functions.md) for more information.

### Control execution flow

For an imperative language, providing a way to control execution flow is important.

Back to the top, there is a case mentioned that
'we would like to use large computing resources when we're handling data in the first partition since the amount of data there is far larger than that in the other partitions'.
In order to implement this in ETL, we need to control the execution flow to configure a large computing resource.

A common way in general programming language to handle this is to provide some 'if' statement.
And we need to provide a condition expression for 'if' statement.
The inner action of 'if' statement is then executed according to the true or false value of the condition expression.

Easy SQL provides a similar way to control execution flow.
We can control if a step needs to be executed by providing a 'if' statement after any step definition.
Below is an example:

```sql
-- target=func.do_some_thing(), if=bool()
-- target=func.do_another_thing(), if=bool(1)
-- target=func.do_a_third_thing(), if=bool(${some_variable_indicator})
-- target=temp.table_a, if=bool()
select * from some_db.table_a
-- target=variables, if=bool(1)
select 1 as a
```

From the example, we know the following things about 'if' statement in Easy SQL:

- There must be a function call following the 'if' statement.
- The function call must return a boolean value to indicate if the step needs to be executed.
- Any step could be controlled by a 'if' statement, including 'func' 'temp' 'variables' and so on.

### Write data

If we need to write data to some table, we could use another type of target. The name of the target is 'output'.
There should be a query statement following the 'output' target. And the result of the query will be written to the output table.

Below is an example:

```sql
-- target=temp.result
select 1 as a, '2' as b

-- target=output.some_db.some_table
select * from result
```

After the execution of the ETL above, there will be one row written to table 'some_db.some_table'.

Things to note about 'output' target:

- There must be a full table name (both database name and table name specified) after the 'output' keyword in the target definition.
- The table must be created before writing data.
- If we'd like to create table automatically, we need to define a special variable named '\_\_create_output_table\_\_' with value equals to 1.
- If we'd like to write data to some static partition of the output table, we need to define a special variable named '\_\_partition\_\_' with partition column name followed by. An example could be '__partition__data_date'. Then the partition column is 'data_date'. The value of the variable will be the partition value when writing data.
- If we'd like to write data to some static partition of the output table, we can only define one partition value at the moment.
- If the query returns more columns than what is defined by the real table, the extra columns will be ignored.
- If the query returns less columns than what is defined by the real table, an error will be raised.

### Templates used to reuse code

To support reusing of code, templates have been introduced in Easy SQL.
Templates are similar to functions in general programming languages.
Functions could be called anywhere while templates could be used anywhere as well. This way, code is reused.

Just like functions, there are name, parameters and body for a template as well.

Below is a concrete example of templates:

```sql
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
    left join order_count oc on @{join_conditions(right_table=oc)}
    left join sales_amount sa on @{join_conditions(right_table=sa)}
```

There are two templates named 'dim_cols' and 'join_conditions' defined.
One with no parameters and one with one parameter named 'right_table'.

This example is about a very common case when we'd like to merge two tables with the same dimension columns.
After the template is used, the dimension column names and join conditions are reused, just like how we reuse functions in general programming language.

From the example above, we could find a few things to note about templates:

- Template is a step in ETL. It is defined by a target with name 'template' and a following descriptive name. The descriptive name is the template name.
- The body of a template could be anything.
- If there are template parameters, no need to declare them, just use them by '#{PARAMETER_NAME}'. Easy SQL will extract these parameters for you at runtime.
- Templates could be used in any target with syntax '@{TEMPLATE_NAME}'. If there are template parameters, we need to pass them as named parameters.

Besides, there are some other notes:

- Variables can be referenced in template body, and the resolution of variables happens at the resolution time of the template (when the step with template reference is executing). This is useful since we can change the value of some variable between two targets referencing the same template.
- There should be no templates used in the body of templates. This is to make the resolution of templates to be simple.

### Include other ETL code snippets

Template is designed for reusing code within one ETL. How to reuse code across ETLs?
One common way to reuse code is to create a temporary mid-table.
But it seems heavy since we need to create a real table and there might be data copying.

Easy SQL provides a way to reuse code from some other ETL file. This is the 'include' command.

Include looks similar to target. Below is an example:

```sql
-- include=snippets/some_snippet.sql
```

The file path is a path relative to the current working directory.

When Easy SQL processed the 'include' command, the content of the file will be expanded.
The result will be the same as when we write code in here directly.

For the example above, if we have the following content in `some_snippets.sql`:

```sql
-- target=temp.some_table
select * from some_db.some_table
-- target=template.some_columns
a, b, c
```

Then the content of the ETL will be the same as the content of `some_snippets.sql` since there is only one include command there.

Notes about 'include' command:

- Include command could be used at any line of code.
- When Easy SQL processed this 'include' command, the content of the file will simply be expanded.

### Debugging support

In a complicated ETL, it is easy to introduce bugs.
A general programming language usually provides some ways to help with debugging.
The most commonly used way is about logging and assertion.

Developers can log variables anywhere to provide information about the executing step.
They can also set an assertion if there is any important assumption made in the following code.

To do logging and assertion in Python, the code looks like below:

```python
logger.info(f'some thing happened, check the variables: var_a={var_a}')
assert var_a == 'something assumed', f'var_a is not as assumed: var_a={var_a}'
```

Easy SQL provides a similar way to do logging and assertion. They're both provided by a type of target.
Check the example below to see its usage.

```sql
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
```

From the example above, we know that:

- When using the 'log' target, we need to specify a message about what to log.
- The log message format is the same as a variable. I.e. It should be composed of chars '0-9a-zA-Z_'.
- There should be exactly one row returned from the query of some 'log' target. If there is more than one row returned, only the first row will be logged.
- There are two formats of 'check' target. One is to specify a check message with a query. The other is to call a function, which returns a boolean value.
- When the 'check' target is used as a message with a query, the returned value of the query must be one row with two columns named 'actual' and 'expected'.

### Debugger interface

There is a debugger interface provided by Easy SQL. It could be used with `Jupyter` to debug interactively. Follow the steps below to start debugging.

1. Install `Jupyter` first with command `pip install jupyterlab`.
2. Create a file named `debugger.py` with contents like below:
(A more detailed sample could be found [here](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor_debugger/index.html).)

```python
from typing import Dict, Any

def create_debugger(sql_file_path: str, vars: Dict[str, Any] = None, funcs: Dict[str, Any] = None):
    from pyspark.sql import SparkSession
    from easy_sql.sql_processor.backend import SparkBackend
    from easy_sql.sql_processor_debugger import SqlProcessorDebugger
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    backend = SparkBackend(spark)
    debugger = SqlProcessorDebugger(sql_file_path, backend, vars, funcs)
    return debugger
```
3. Create a file named `test.sql` with contents as [here](https://github.com/easysql/easy_sql/blob/main/test/sample_etl.spark.sql).
4. Then start jupyter lab with command: `jupyter lab`.
5. Start debugging like below:

![ETL Debugging](https://raw.githubusercontent.com/easysql/easy_sql/main/debugger-usage.gif)

For details of how to debug interactively, please view the [debug](debug.md) page.

For details of the APIs, we can refer to API doc [here](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/sql_processor_debugger/index.html).

### Write data

If we need to write data to some table, we could use another type of target. The name of the target is 'output'.
There should be a query statement following the 'output' target. And the result of the query will be written to the output table.

Below is an example:

```sql
-- target=temp.result
select 1 as a, '2' as b

-- target=output.some_db.some_table
select * from result
```

After the execution of the ETL above, there will be one row written to table 'some_db.some_table'.

Things to note about 'output' target:

- There must be a full table name (both database name and table name specified) after the 'output' keyword in the target definition.
- The table must be created before writing data.
- If we'd like to create tables automatically, we need to define a special variable named '\_\_create_output_table\_\_' with value equals to 1.
- If we'd like to write data to some static partition of the output table, we need to define a special variable named '\_\_partition\_\_' with partition column name followed by. An example could be '__partition__data_date'. Then the partition column is 'data_date'. The value of the variable will be the partition value when writing data.
- If we'd like to write data to some static partition of the output table, we can only define one partition value at the moment.
- If the query returns more columns than what is defined by the real table, the extra columns will be ignored.
- If the query returns less columns than what is defined by the real table, an error will be raised.

### List variables

There are list variables supported in Easy SQL as well.

List variables are different from variables mentioned previously.
The main difference is that the values of these variables are lists.
So that list variables could not be used in SQL statements, since we cannot simply convert a list to a string and do variable resolution.

List variables can only be used as function parameters right now.

Below is an example of list variables.

```sql

-- target=list_variables
select explode(array(1, 2, 3)) as a

-- target=func.print_list_variables(${a})
```

If we have function implementation like below:

```python
def print_list_variables(a: List[str]):
    print(a)
```

Then the function output will be: `[1, 2, 3]`

### SQL actions

There are some cases where we'd like to just execute some SQL statement without anything to do about its result. We can use 'action' in these cases.

This usually happens when we want to execute some DDL statement. Examples would be like to create table, to drop partition of some table etc.

Action is a type of target as well and it follows target syntax. Below is an example of 'action' target:

```sql
-- target=action.create_some_table
create table some_table (
    id int
    , value string
)
```

Things to note about actions:

- There should be a descriptive name for an action. The name should be composed of chars '0-9a-zA-Z_' and follow the 'action' keyword.
- In the body of an action target, templates and variables can be used as in any other target.
