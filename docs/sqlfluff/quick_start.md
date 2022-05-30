# Quick start
## Introduction for sqlfluff
With multiple contributors to a project and varying SQL backgrounds, it's really difficult to maintain consistent readability and comprehension across a codebase. sqlfluff is a tool that can easily checkout sql code quality with varying SQL backgrounds and dialects.


## Python Requirement
Sqlfluff not support python 2 , it need python with 3.x.x. Check your python version with the following:
```bash
$ python --version
```


## Quick install 
Install sqlfluff with pip:
```bash
$ pip install sqlfluff
```
Check install is success:
```bash
$ pip install sqlfluff
```


## Hands on demo
We can use command line to quick check the sql code quality. Here we use an example, save the example to a sql file.
```sql
SELECT a+b  AS foo,
c AS bar from my_table
```
cd to the folder and run the test
```bash
$ cd /test/doc
$ sqlfluff lint test_sqlfulff.sql --dialect ansi
```
output:
```
== [test_sqlfulff.sql] FAIL                                                                                                                                                                                             
L:   1 | P:   1 | L034 | Select wildcards then simple targets before calculations
                       | and aggregates.
L:   1 | P:   1 | L036 | Select targets should be on a new line unless there is
                       | only one select target.
L:   1 | P:   9 | L006 | Missing whitespace before +
L:   1 | P:   9 | L006 | Missing whitespace after +
L:   1 | P:  11 | L039 | Unnecessary whitespace found.
L:   2 | P:   1 | L003 | Expected 1 indentations, found 0 [compared to line 01]
L:   2 | P:  10 | L010 | Keywords must be consistently upper case.
L:   2 | P:  23 | L009 | Files must end with a single trailing newline.
All Finished 📜 🎉!
```
The sqlfluff checker will tell what is need to take care. To further understand the ruel can check https://docs.sqlfluff.com/en/stable/rules.html#ruleref

Automatically fix the issue with specify rule:
```bash
$ sqlfluff fix test_sqlfulff.sql --rules L003,L009,L010 --dialect ansi
```


## Customer style
In lint command can specify to use different kind of dialect for varying SQL backend. Check the currently support backend:

```bash
$ sqlfluff dialects
```
```output
==== sqlfluff - dialects ====
ansi:                 ansi dialect [inherits from 'nothing']
bigquery:            bigquery dialect [inherits from 'ansi']
db2:                      db2 dialect [inherits from 'ansi']
exasol:                exasol dialect [inherits from 'ansi']
```
All dialects is inherited from basic ansi dialects. If want customer dialects need fork the git and create new class.


In lint command also can specify to use different rules to check.Check the currently support rules:
```bash
$ sqlfluff rules
```
```output
==== sqlfluff - rules ====
L001: Unnecessary trailing whitespace.                                          
L002: Mixed Tabs and Spaces in single whitespace.                               
L003: Indentation not consistent with previous lines.   
```
Rules is predefined but it is flexible at parameter and usage. All the settings can be specify in .sqlfluff file. Change the config file, the customer style will take action immediately.

Only enable few rule:
```config
rules = L001,L002 (default :all)
```
Ignore specify rule: 
```config
exclude_rules = L001,L002
```
customerize for specify rule,parameters are predefined:
```config
[sqlfluff:rules:L010]
capitalisation_policy = consistent
ignore_words = from
ignore_words_regex = None
```

## Jinjia Template
SQL fluff also support template replace by variables for flexibility.By default it is Jinjia template, and we also use jinjia as example here.
```sql
SELECT a+b  AS foo,
c AS bar from my_table where name = {{ test_name }}; 
```
As I try the parameter holder as ${test_name} cannot be parsed, format is compulsory to follow {{ }}.

Then u need to give the value of the parameter that will be format in the config file .sqlfluff

```config
[sqlfluff:templater:jinja:context]
test_name=456
```
After this in command line can get the parse result by run the following:
```bash
$ sqlfluff parse test_sqlfulff.sql --rules L003,L009,L010 --dialect ansi
```





# Easy sql integration Plan
1. Parse the backend config in easy sql to define the dialect
2. Our easy sql have multiple sql with comment as seperator. Add forloop to loop through all different sql file
3. Make rules enable /disable for different sql backend. For example, bigquery specific need schema.
4. Add rules to check, including input/output check, partition check
5. Allow for easy_sql function  and variable like ${temp_db} to be checked.


refer dbt tool but it is static


