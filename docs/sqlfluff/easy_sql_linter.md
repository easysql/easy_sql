# Easy Sql Linter
Easy sql is a powerful tool that can bring convenience to ETL developer. But so far we do not have a easy sql grammar supported compiler that can auto check sql quality and auto fix the violations. That is the reason why we develop such linter tool on sqlfluff. By use this linter, we can apply any easysql grammar query to it.


## Command Line Interface
The command line interface is by running the following :
```bash
$ python easy_sql/sql_linter/sql_linter_cli.py fix --path ${path}
```
It has fix or lint mode, for lint it will only show the violation while for fix it will write out the fixed query. 

fix parameter:
+ path : the absolute location of the query 
+ include: comma separated rule id to be included
+ exclude: comma separated rule id to be excluded
+ backend: the special running backend for the query, it will impact the rules that take into action
+ easysql: bool value to infer whether it is easy sql grammar or normal grammar
+ inplace: bool value to infer whether overwrite the origin query file for the fixed output. If false it will write to new file with .fixed.sql 

lint parameter:
+ path : the absolute location of the query
+ include: comma separated rule id to be included
+ exclude: comma separated rule id to be excluded
+ backend: the special running backend for the query, it will impact the rules that take into action
+ easysql: bool value to infer whether it is easy sql grammar or normal grammar

## Code usage
```python
from easy_sql.sql_linter.sql_linter import SqlLinter
sql= ""
sql_linter = SqlLinter(sql,
                       include_rules=None,
                       exclude_rules=None)
result = sql_linter.lint("bigquery",easysql=False)
fixed = sql_linter.fix("bigquery",easysql=False)

```
You may find out that in the lint and fix command , we are given the sql grammar. This is to enable the swithc of grammar. If you do not provided the grammar and you are using easy sql, it will automatically detect the gramma from easysql, if you are using normal sql and do not provide grammar, it will bring bugs in the end.

The grammar also impact the applied rules. If define as bigquery, for all the self-define rules that groups have bigquery and sqlfluff inbuild core rules will take into action. 

```python
 # in self-define rule
groups = ("all", "bigquery")
```