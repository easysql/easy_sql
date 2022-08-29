from __future__ import annotations

import re
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from pyspark.sql.types import DataType


class UDF:
    def __init__(self, func: Callable, return_type: DataType):
        self.func = func
        self.return_type = return_type

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


def check_regex_func(pattern):
    return lambda any_str: any_str if any_str and re.match(pattern, any_str) else None
