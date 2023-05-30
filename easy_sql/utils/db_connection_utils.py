from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Optional

from easy_sql.utils.kv import KV

if TYPE_CHECKING:
    from sqlalchemy.engine.base import Connection, Engine

    from easy_sql.sql_processor.backend.flink import FlinkBackend


def _create_sqlalchemy_conn(flink_connector_config: Dict[str, str]) -> Optional[Connection]:
    base_url = flink_connector_config["'url'"].strip("'")
    username = flink_connector_config["'username'"].strip("'")
    password = flink_connector_config["'password'"].strip("'")
    split_expr = "://"
    split_expr_index = base_url.index(split_expr)
    db_type = base_url[len("jdbc:") : split_expr_index]
    sqlalchemy_db_url = f"{db_type}{split_expr}{username}:{password}@{KV.from_config(base_url, split_expr).v}"
    if sqlalchemy_db_url:
        from sqlalchemy import create_engine

        engine: Engine = create_engine(sqlalchemy_db_url, isolation_level="AUTOCOMMIT", pool_size=1)
        conn: Connection = engine.connect()
        return conn


def get_connector_raw_conn_for_flink_backend(backend: FlinkBackend, connector_name: str) -> Optional[Connection]:
    connector_options = backend.flink_tables_config.get_connector_options(connector_name)
    return _create_sqlalchemy_conn(connector_options)
