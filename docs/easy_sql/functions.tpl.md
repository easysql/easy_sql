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

{{ spark functions }}

{{ rdb functions: Functions for rdb backend (PostgreSQL Clickhouse BigQuery) }}
