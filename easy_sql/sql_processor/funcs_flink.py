from __future__ import annotations

from typing import TYPE_CHECKING

from .funcs_common import AlertFunc, AnalyticsFuncs, ColumnFuncs, TableFuncs

if TYPE_CHECKING:
    from .backend import FlinkBackend


__all__ = [
    "StreamingFuncs",
    "ColumnFuncs",
    "ParallelismFuncs",
    "AlertFunc",
    "TableFuncs",
    "AnalyticsFuncs",
]


class ParallelismFuncs:
    def __init__(self, flink: FlinkBackend):
        self.flink = flink

    def set_parallelism(self, partitions: str):
        try:
            int(partitions)
        except ValueError:
            raise Exception(f"partitions must be an int when repartition a table, got `{partitions}`")
        self.flink.flink.get_config().set("table.exec.resource.default-parallelism", partitions)


class StreamingFuncs:
    def __init__(self, flink: FlinkBackend):
        self.flink = flink

    def execute_streaming_inserts(self):
        self.flink.execute_streaming_inserts()
