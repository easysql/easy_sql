# Functions

While most of the ETL code is SQL, sometimes we would like to do things that are difficult to implement in SQL.

For example:

- Send out a http request to report status when some step of the ETL fails for some reasons.
- Check if a partition exists.
- Get the first partition value. 

These tasks could be easily implemented in python functions.

Easy SQL provides a way to call functions in ETL code. And there are a bunch of useful python functions provided by Easy SQL.
These functions are widely used in ETL development.

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

{{ spark functions }}

{{ rdb functions: Functions for rdb backend (PostgreSQL Clickhouse BigQuery) }}
