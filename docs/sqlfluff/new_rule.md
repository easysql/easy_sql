# Easy SQL customized code quality rules

## Introduction

In SQL FLuff we already have predefined rules, it follows the common rules for clean SQL code.
You can check all the implemented rule from running the command: `sqlfluff rules`.
But they are not enough, in some situation we need to implement a customized rule. This documentation will go through the steps of how to achieve it.

Before we start, we need to understand the design of SQL Fluff first. When checking the SQL code, it will go through the following steps:

- **templates handling**: To replace the variable used in SQL. Jinjia/dbt format are supported. The replacement is static, and all the variables will be resolved from the config file first.
- **lex**: Separate the SQL into whitespace and code segment.
- **parse**: Parse the lex result and organize the tokens to a grammar tree with the specific SQL dialects. If no matches found for a segment, the content will be wrapped in an `UnparsableSegment` which will be picked up as a parsing error later.
- **lint**: Walk through the parsed tree-structured data and check if there are violations according to the rules. There will be `lintError` returned if any violations found.
- **fix**: Auto-fix the problem pointed out by lint.

In SQL Fluff, segments form a tree-like structure. The top-level segment is a `FileSegment`, which contains zero or more `StatementSegment`s.
Before parsing the segments and names according to their type, they are `raw`, meaning that they are literal values.

## New rule

To create new rules, we need to implement a new class extended from `BaseRule` in SQL Fluff first.
The name of the class will become the name of rules. The name convention should be `Rule_xxxxx_Lxxx`. The `BaseRule` contains a parsing logic to parse rules following the convention.
The core function of lint is `_eval`, the input is a tree structure element to indicate the context.
By calling context.segment.children you can find the next segment, and the link structure also allows you traverse to the end.

In the `groups` definition, "all" must be there.

```config
# Comma separated list of rules to check, default to all
rules = all
```

You can check the type of the segment by `is_type("table_reference")`. To find the correct name of the segment, you need to go into SQL Fluff code to find the segment class and name is inside.
With the `is_type` check, you can correctly point to the location that need to check the rule。

```python
from sqlfluff.core.rules.base import BaseRule, RuleContext


class Rule_BigQuery_L001(BaseRule):

    groups = ("all", "bigquery")

    def __init__(self, *args, **kwargs):
        """Overwrite __init__ to set config."""
        super().__init__(*args, **kwargs)

    def _eval(self, context: RuleContext):
        pass
```

```python
from sqlfluff.core.parser import BaseSegment

class WildcardExpressionSegment(BaseSegment):
    type = "wildcard_expression"
```

## Define the rule violation

The return value of the _eval() function is the rule violation record object.
If the check passed and no error found, it should return nothing.
The return object is a `LintResult` object. In creation of this object, you should pass three arguments.

+ `anchor`: the segment that hold the position info.
+ `description`: a description of the reason why it is failed.
+ `fix`: a list of fix object (`delete`/`replace`/`create_before`/`create_after`) to fix the problem. To further understand the function, can read the `LintFix` code. If you do not pass in anything as `fix`, it will do nothing in the fix step.


## Add the rule into action

To make the customized function work, you need to pass in a list of classes as the linter `user_rules` parameter.
So far, we imported all the classes in the `easy_sql.sql_linter.rules`. By adding the rule while initiating the module, it works as expected.

```python
from sqlfluff.core import Linter

linter = Linter( user_rules=[])
```

```python
from easy_sql.sql_linter.rules.bq_schema_rule import Rule_BigQuery_L001

__all__ = [Rule_BigQuery_L001]
```
