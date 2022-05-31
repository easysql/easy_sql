"""An example of a custom rule implemented through the plugin system."""

from sqlfluff.core.rules.base import (
    BaseRule,LintFix,
    LintResult,
    RuleContext,
)
from sqlfluff.core.parser import CodeSegment

class Rule_BigQuery_L001(BaseRule):
    """select from is compulsory to have schema
    **Anti-pattern**
    use select from table without schema
    .. code-block:: sql
        SELECT *
        FROM foo
    **Best practice**
    Do not order by these columns.
    .. code-block:: sql
        SELECT *
        FROM test.foo
    """

    groups = ("all", "bigquery")

    def __init__(self, *args, **kwargs):
        """Overwrite __init__ to set config."""
        super().__init__(*args, **kwargs)

    def _eval(self, context: RuleContext):
        """check from table have schema"""
        if context.segment.is_type("table_reference"):
            if len(context.segment.segments) != 3:
                return LintResult(
                    anchor=context.segment,
                    fixes=[
                        LintFix.create_before(
                            context.segment,
                            [CodeSegment(raw="temp_db.")],
                        )
                    ],
                    description=f"select from `{context.segment.raw}` do not have schema in table.",
                )

