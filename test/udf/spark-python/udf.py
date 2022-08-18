from typing import List

__all__ = ["string_set"]


def string_set(string_arr: List[str]) -> List[str]:
    return list(set(string_arr))
