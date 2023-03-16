from __future__ import annotations

from typing import TYPE_CHECKING

from sqlfluff.core.parser import CodeSegment
from sqlfluff.core.rules.base import BaseRule, LintFix, LintResult
from sqlfluff.core.rules.crawlers import SegmentSeekerCrawler

if TYPE_CHECKING:
    from sqlfluff.core.rules.context import RuleContext


class Rule_BigQuery_L001(BaseRule):
    """
    Table schema is required for queries in BigQuery.

    **Anti-pattern**
    Select from some table without schema.

    .. code-block:: sql
        SELECT *
        FROM foo

    **Best practice**
    Select from some table with schema.

    .. code-block:: sql
        SELECT *
        FROM test.foo
    """

    groups = ("all", "bigquery")
    crawl_behaviour = SegmentSeekerCrawler({"table_reference"})

    def __init__(self, *args, **kwargs):
        """Overwrite __init__ to set config."""
        super().__init__(*args, **kwargs)

    def _eval(self, context: RuleContext):
        """check from table have schema"""
        if len(context.segment.segments) != 3:
            return LintResult(
                anchor=context.segment,
                fixes=[
                    LintFix.create_before(
                        context.segment,
                        [CodeSegment(raw="${temp_db}.")],
                    )
                ],
                description=f"No schema found when select from table `{context.segment.raw}`.",
            )
