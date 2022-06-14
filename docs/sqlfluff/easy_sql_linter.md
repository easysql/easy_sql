# Easy SQL Linter

Easy SQL is a powerful tool that can bring convenience to ETL developer.
But so far we do not have an easy SQL grammar supported compiler that can auto-check SQL quality and auto fix the violations.
It is the reason why we develop such linter tool on top of sqlfluff. With this linter, we can do static analysis and auto-fixing of ETL code written in Easy SQL.


## Command Line Interface

The command line interface usage is as follows:

```bash
$ python3 -m easy_sql.sql_linter.sql_linter_cli fix --path ${path}
```

There are fix and lint mode, for lint it will only show the rule violations while for fix it will auto-fix the query. 

Fix mode parameters:

- path: The location of the ETL file.
- include: Comma separated rule id to be included.
- exclude: Comma separated rule id to be excluded.
- backend: The backend of the ETL file, it will be used to find the correct rules.
- easy_sql: Boolean value to indicate whether the ETL file is written in Easy SQL or normal SQL. Will default to true.
- inplace: Boolean value to indicate whether to overwrite the origin file with the fixed output. If false the fixed output will be written to a new file with suffix `.fixed.sql`. 

Lint mode  parameters:

- path: The location of the ETL file.
- include: Comma separated rule id to be included.
- exclude: Comma separated rule id to be excluded.
- backend: The backend of the ETL file, it will be used to find the correct rules.
- easy_sql: Boolean value to indicate whether the ETL file is written in Easy SQL or normal SQL. Will default to true.

## Programmatical usage

```python
from easy_sql.sql_linter.sql_linter import SqlLinter

sql = ""
sql_linter = SqlLinter(sql, include_rules=None, exclude_rules=None)
result = sql_linter.lint("bigquery", easy_sql=True)
fixed = sql_linter.fix("bigquery", easy_sql=True)
```

You may find out that in the lint and fix command there is an option to specify which backend the ETL file is written to.
If you do not provide the option, and you are using easy sql, it will automatically detect the backend from the file.
Make sure you've specified the correct options, or it will generate unexpected output.

(
For developers:

The backend impacts the applied rules. If defined as bigquery, all the customized rules with groups containing bigquery and sqlfluff built-in core rules will be applied.

```python
# groups in customized rules
groups = ("all", "bigquery")
```
)
