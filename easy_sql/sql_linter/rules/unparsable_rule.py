"""An example of a custom rule implemented through the plugin system."""

from sqlfluff.core.rules.base import (
    BaseRule,
    RuleContext,
)

class Rule_Common_L001(BaseRule):
    """This rule is common for all backend to say that have element
    that cannot find out value for parse
    """

    groups = ("all", "bigquery")

    def __init__(self, *args, **kwargs):
        """Overwrite __init__ to set config."""
        super().__init__(*args, **kwargs)
        self.force_schema = True

    def _eval(self, context: RuleContext):
        # the check is outside , this class is only used for hold the error info
        pass

