# Add Backend

##Introduction

Easy-sql is designed as a tool to quick implement with different kind of sql backend.

So far supported backends are:

+ spark sql(spark engine)
+ bigquery(sqlalchemy engine)
+ postgresql(sqlalchemy engine)
+ clickhouse(sqlalchemy engine)
+ flink(flink engine)

Easy sql is designed to be flexible and scalable. If in future have requirement on build new backend engine in easy sql, it can be easily added on by implement the method. Following is the description on how to implement new engine step by step.

##
