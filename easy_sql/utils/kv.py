from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple


def get_key_by_splitter_and_strip(source: str, splitter: Optional[str] = "=", strip_chars: Optional[str] = None):
    source = source.strip()
    return source[: source.index(splitter or "=")].strip(strip_chars)


def get_value_by_splitter_and_strip(source: str, splitter: Optional[str] = "=", strip_chars: Optional[str] = None):
    source = source.strip()
    splitter = splitter or "="
    return source[source.index(splitter) + len(splitter) :].strip(strip_chars)


class KV:
    def __init__(self, k: str, v: str) -> None:
        self.k, self.v = k, v

    @staticmethod
    def from_config(config_line: str, splitter: Optional[str] = "=", strip_chars: Optional[str] = None) -> KV:
        return KV(
            get_key_by_splitter_and_strip(config_line, splitter, strip_chars),
            get_value_by_splitter_and_strip(config_line, splitter, strip_chars),
        )

    def as_tuple(
        self, k_convert: Optional[Callable[[str], Any]] = None, v_convert: Optional[Callable[[str], Any]] = None
    ) -> Tuple[Any, Any]:
        return (k_convert(self.k) if k_convert else self.k, v_convert(self.v) if v_convert else self.v)

    def as_dict(self) -> Dict[str, str]:
        return {self.k: self.v}
