# Self-define rule

## Introduction
In SQL FLuff we already have rules that is predefined, it follows the common rules for clean SQL code. You can check all the implemented rule from running the command : sqlfluff rules . But that is not enough, in some situation we need implement a self-defined rule. This documentation will go through the step about how to make it.

Before we start let us understand the design structure of SQL Fluff first. For each of the check SQL it will go through the following steps:
+ ####templates  
    replace the variable refer to the configuration. Support Jinjia/dbt format. But the replacement is static, and all the variable value should be place in config file first.
+ ####lex  
    separate the SQL into whitespace and code segment,  No meaning is imparted; that is the job of the parser.
+ ####parse
    Parse the lex result and add grammar on it with specific SQL dialects.#. If no match is found for a segment, the contents will be wrapped in an UnparsableSegment which is picked up as a parsing error later.
+ ####lint
    walk through the parsed tree structure data and check whether have violation of the rules. If have will return lintError.
+ ####fix
    After lint the grammar errors are pointed out, in the fix step it will auto-fix the problem by the fix step that already define in lintError.

In SQLFluff, segments form a tree-like structure. The top-level segment is a FileSegment, which contains zero or more StatementSegments, and so on. Before the segments have been parsed and named according to their type, they are ‘raw’, meaning they have no classification other than their literal value.


## New Rule
To create new rules, you need first implement a new class extend from BaseRule in SQLFluff. The name of the class will become the name of rules, and the BaseRule will have a parse logic so the class name is compulsory to be Rule_xxxxx_Lxxx. The core function of lint is define in the _eval, the input is a tree structure element indicate the context. By calling context.segment.children you can find the next segment, and this link structure also allows you traverse to end.

In the groups definition, it is compulsory to have "all", and for other context you can name it as you want. This will take action in the context name given in config rules.

```config
# Comma separated list of rules to check, default to all
rules = all
```

You can check the type of the segement by is_type("table_reference"), the correct name of the segment you need go into SQL Fluff code to find the segment class and name is inside. By the is_type check, you can correctly point to the location that need to check the rule。
```python
from sqlfluff.core.rules.base import BaseRule,RuleContext

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
The return value in the _eval() function is the rule violation record object, if the check is pass and no error it should return nothing. The return object is LintResult class. In create of this class, you should pass three info.
+ anchor: the segment that hold the position info.
+ description: the string statement to refer the reason why it is failed
+ fix: a list of fix object(delete/replace/create_before/create_after) to fix the problem. To further understand the function, can read the LintFix class. If you do not place any stuff to the fix parameter, it will do nothing regarding this rule in the fix step.


## Add the rule into action
To make the self-define function take action, you need to put list of classes into the linter user_rules parameter. So far, we import all the classes in the easy_sql.sql_linter.rules. By add the rule into the init python, it will works.

```python

from sqlfluff.core import Linter
linter = Linter( user_rules=[])
```
```python
from easy_sql.sql_linter.rules.bq_schema_rule import Rule_BigQuery_L001

__all__ = [Rule_BigQuery_L001]
```


