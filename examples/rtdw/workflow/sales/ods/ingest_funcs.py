from __future__ import annotations

import json
from typing import TYPE_CHECKING, Dict

from easy_sql.sql_processor.backend.base import SaveMode, TableMeta

if TYPE_CHECKING:
    from easy_sql.sql_processor.backend import FlinkBackend


__all__ = ["ingest_cdc_pg"]


def ingest_cdc_pg(backend: FlinkBackend, db: str, connector: str, table_list: str, domain: str):
    db_config = backend.flink_tables_config.database(db)
    if not db_config:
        raise Exception("Db not configured: " + db)
    connector_config = backend.flink_tables_config.connector(db_config, connector_name=connector)
    if not connector_config:
        raise Exception(f"Connector {connector} not configured for db {db}")
    target_tables = {table: f'ods_{domain}.{domain}_{table.split(".")[1]}' for table in table_list.split(",")}
    table_with_fields_list = [
        {
            "name": table,
            "schemaRefTableName": target_table,
            "fields": backend.flink_tables_config.table_fields(target_table, ["_di"]),
        }
        for table, target_table in target_tables.items()
    ]

    backend.register_tables(list(target_tables.values()), False)

    from py4j.java_gateway import java_import
    from pyflink.java_gateway import get_gateway

    gw = get_gateway()
    java_import(gw.jvm, "com.easysql.example.Sources")
    readPgCDC = eval(
        "gw.jvm.com.easysql.example.Sources.readPgCDC",
        {
            "gw": gw,
        },
    )
    _j_env = backend.flink_stream_env._j_stream_execution_environment  # type: ignore
    result_tables: Dict[str, str] = readPgCDC(
        _j_env, backend.flink._j_tenv, connector_config["options"], json.dumps(table_with_fields_list)
    )

    ingest_tables = {
        f'ods_{domain}.{domain}_{table.split(".")[1]}': read_temp_table
        for table, read_temp_table in result_tables.items()
    }
    for hudi_table, read_temp_table in ingest_tables.items():
        backend.exec_native_sql_query(f"select * from {read_temp_table}").print_schema()
        table_with_partition = backend.exec_native_sql_query(
            f"select *, from_unixtime(_op_ts / 1000, 'yyyyMMdd') as _di from {read_temp_table}"
        )
        backend.flink.create_temporary_view(read_temp_table, table_with_partition)
        assert "." not in read_temp_table
        backend.save_table(TableMeta(read_temp_table), TableMeta(hudi_table), SaveMode.append)
