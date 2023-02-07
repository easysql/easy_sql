from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Optional

from easy_sql.utils.kv import KV

if TYPE_CHECKING:
    from sqlalchemy.engine.base import Connection, Engine

    from easy_sql.sql_processor.backend.flink import FlinkBackend


def _create_sqlalchemy_conn(flink_connector_config: Dict) -> Optional[Connection]:
    if flink_connector_config and flink_connector_config["options"]["connector"] == "jdbc":
        base_url = flink_connector_config["options"]["url"]
        username = flink_connector_config["options"]["username"]
        password = flink_connector_config["options"]["password"]
        split_expr = "://"
        split_expr_index = base_url.index(split_expr)
        db_type = base_url[len("jdbc:") : split_expr_index]
        sqlalchemy_db_url = f"{db_type}{split_expr}{username}:{password}@{KV.from_config(base_url, split_expr).v}"
        if sqlalchemy_db_url:
            from sqlalchemy import create_engine

            engine: Engine = create_engine(sqlalchemy_db_url, isolation_level="AUTOCOMMIT", pool_size=1)
            conn: Connection = engine.connect()
            return conn


def get_table_raw_conn_for_flink_backend(backend: FlinkBackend, table: str) -> Optional[Connection]:
    _, _, connector = backend.get_table_config_and_connector(table)
    return _create_sqlalchemy_conn(connector) if connector and table else None


def get_connector_raw_conn_for_flink_backend(
    backend: FlinkBackend, db_name: str, connector_name: str
) -> Optional[Connection]:
    connector = backend.get_db_connector(db_name, connector_name)
    return _create_sqlalchemy_conn(connector) if connector else None
