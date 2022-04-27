# Easy SQL

Easy SQL is built to ease the data ETL development process.
With Easy SQL, you can develop your ETL in SQL in an imperative way.

It defines a few simple syntax on top of standard SQL, with which SQL could be executed one by one.
Easy SQL also provides a processor to handle all the new syntax.

Since this is SQL agnostic, any SQL engine could be plugged-in as a backend.
There are built-in supported for several popular SQL engines, including SparkSQL, PostgreSQL, Clickhouse, Aliyun Maxcompute, Google BigQuery.
More will be added in the near future.

## Background

Why do we need imperative syntax in ETL?

SQL is designed to be used in a declarative way and it causes a few troubles when we use SQL to develop complicated ETL.

Think about the following cases.

1. We would like to use large computing resources when we're handling data in the full-data partition since the amount of data there is far larger than that in the other partitions.
2. We would like to send out a http request to report status when some step of the ETL fails for some reasons(E.g. some data does not conform to the previous assumptions).
3. We would like to reuse some code to check if some order is a valid order (think about e-commerce business).
4. We would like to stop at some step of the ETL and check if the data is what we expected.

When we use SQL to develop our ETL, it is hard to handle the above cases. 
But for a company with a wide range of data usage, there are similar cases everywhere.

### Why imperative SQL

The above cases could be easily handled if we have an imperative-way to write our code.
This might be the reason why a lot of developers like to write ETLs in a general programming language like Python or Scala.

But for data ETL development case, we still think that to use SQL or SQL-like language is a better choice. The main reasons are:

- Consistent code style across all ETLs.
- All roles in the team can easily understand the logic in ETL.
- All code about one ETL mainly stays in one file and it makes things simpler when we try to read and understand what it does in the ETL.

## Design principal

When first tried to design the syntax, we found several important things. Which are:

- Keep compatible with standard SQL. So that every SQL editor could be used to develop in Easy SQL.
- Try to use SQL-way to implement most of the features.
- Use intuitive syntax which is also similar to the widely-used syntax in other programming languages.
- Implement widely-used debugging features, such as logging and asserting and even step by step debugging.

These important things become the design principals of Easy SQL. They provide guidance in the whole design process.
If there is an argument about which design is better, the design principals could be referred to make a decision.

## Language features in Easy SQL

For Easy SQL, guided by the design principals, there are a few simple language features added to support these imperative characteristics. Below is a list about these features:

- An imperative structure of ETL code.
- Variables which could be defined and modified any time.
- A way to call external functions.
- A way to control whether a step should be executed.
- Templates that could be reused in the same ETL file.
- Include command that could be used to reuse code at file level.
- Logging and assertion that could be used for debugging.
- A debugger interface.
