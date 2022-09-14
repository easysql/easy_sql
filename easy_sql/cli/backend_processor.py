from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from easy_sql.cli.sql_config import (
    KV,
    EasySqlConfig,
    FlinkBackendConfig,
    SparkBackendConfig,
    parse_vars,
)
from easy_sql.logger import logger
from easy_sql.sql_processor import SqlProcessor
from easy_sql.sql_processor.backend import Backend

if TYPE_CHECKING:
    from sqlalchemy.engine.base import Connection, Engine


class BackendProcessor:
    def __init__(self, config: EasySqlConfig) -> None:
        self.config = config

    @staticmethod
    def create_backend_processor(config: EasySqlConfig) -> BackendProcessor:
        if config.backend == "spark":
            return SparkBackendProcessor(config)
        elif config.backend == "flink":
            return FlinkBackendProcessor(config)
        elif config.backend == "postgres":
            return PostgresBackendProcessor(config)
        elif config.backend == "clickhouse":
            return ClickhouseBackendProcessor(config)
        elif config.backend == "bigquery":
            return BigqueryBackendProcessor(config)
        elif config.backend == "maxcompute":
            return MaxComputeBackendProcessor(config)
        else:
            raise Exception("Unknown backend: " + config.backend)

    def shell_command(
        self, vars_arg: Optional[str], dry_run_arg: str, entry_file: str, backend_config: Optional[List[str]]
    ) -> str:
        raise Exception("No need to construct a shell command for backend " + self.config.backend)

    def run(self, vars: Optional[str] = None, dry_run: bool = False, backend_config: Optional[List[str]] = None):
        backend = self._create_backend(backend_config)

        for prepare_sql in self.config.prepare_sql_list:
            self._exec_prepare_sql(backend, prepare_sql)

        try:
            all_vars = self._pre_defined_vars(backend)
            all_vars.update(parse_vars(vars))
            self._run_with_vars(backend, all_vars, dry_run)
        finally:
            backend.clean()

    def _create_backend(self, backend_config: Optional[List[str]]) -> Backend:
        raise NotImplementedError()

    def _pre_defined_vars(self, backend: Backend) -> Dict:
        return {}

    def _exec_prepare_sql(self, backend: Backend, prepare_sql: str):
        return backend.exec_native_sql(prepare_sql)

    def _run_with_vars(self, backend: Backend, variables: Dict[str, Any], dry_run: bool):
        config = self.config
        sql_processor = SqlProcessor(
            backend, config.sql, variables=variables, scala_udf_initializer=config.scala_udf_initializer
        )
        if config.udf_file_path:
            sql_processor.register_udfs_from_pyfile(config.udf_file_path)
        if config.func_file_path:
            sql_processor.register_funcs_from_pyfile(config.func_file_path)

        sql_processor.run(dry_run=dry_run)


class SparkBackendProcessor(BackendProcessor):
    def __init__(self, config: EasySqlConfig) -> None:
        super().__init__(config)

    def shell_command(
        self, vars_arg: Optional[str], dry_run_arg: str, entry_file: str, backend_config: Optional[List[str]]
    ) -> str:
        config = SparkBackendConfig(self.config, backend_config)

        from_zip_file = False
        cur_file = os.path.abspath(__file__)
        parts = cur_file.split("/")
        for i, part in enumerate(parts):
            if part.endswith(".zip") and os.path.isfile(os.path.join(*parts[: i + 1])):
                from_zip_file = True
                break
        source_folder = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        python_path_arg = f"--python-path '{source_folder}' " if not from_zip_file else ""

        return (
            f'{config.spark_submit} {" ".join(config.spark_conf_command_args())} "{entry_file}" '
            f"-f {self.config.sql_file} --dry-run {dry_run_arg} {python_path_arg}"
            f'{"-v " + vars_arg if vars_arg else ""} '
        )

    def _create_backend(self, backend_config: Optional[List[str]]) -> Backend:
        from easy_sql.spark_optimizer import get_spark
        from easy_sql.sql_processor.backend import SparkBackend

        return SparkBackend(get_spark(self.config.task_name))


class FlinkBackendProcessor(BackendProcessor):
    def __init__(self, config: EasySqlConfig) -> None:
        super().__init__(config)
        self._exec_sql = None

    def shell_command(
        self, vars_arg: Optional[str], dry_run_arg: str, entry_file: str, backend_config: Optional[List[str]]
    ) -> str:
        config = FlinkBackendConfig(self.config, backend_config)
        return (
            f'{config.flink} run {" ".join(config.flink_conf_command_args())} '
            f'--python "{entry_file}" '
            f"-f {self.config.sql_file} --dry-run {dry_run_arg} "
            f'{"-v " + vars_arg if vars_arg else ""} '
        )

    def _create_backend(self, backend_config: Optional[List[str]]) -> Backend:
        from easy_sql.sql_processor.backend import FlinkBackend

        backend = FlinkBackend(self.config.is_batch)
        config = FlinkBackendConfig(self.config, backend_config)

        logger.info(f"Using flink configurations: {config.flink_configurations}")
        backend.set_configurations(config.flink_configurations)

        if config.flink_tables_file_path:
            backend.register_tables(config.flink_tables_file_path, self.config.tables)
            if self.config.tables:
                conn = self.get_conn_from(config.flink_tables_file_path, backend, self.config.tables[0])
                if conn:
                    from easy_sql.sql_processor.backend.rdb import _exec_sql

                    self._exec_sql = lambda sql: _exec_sql(conn, sql)

        return backend

    def _exec_prepare_sql(self, backend: Backend, prepare_sql: str):
        if self._exec_sql:
            self._exec_sql(prepare_sql)
        else:
            logger.warn(
                "Cannot execute prepare-sql: the connector is not configured or with no supported. Will skip this."
            )

    def get_conn_from(self, flink_tables_file_path: str, backend: Backend, table: str):
        from easy_sql.sql_processor.backend import FlinkBackend

        def retrieve_jdbc_url_from(flink_tables_file_path: str, backend: FlinkBackend, table: str):
            if table and flink_tables_file_path and os.path.exists(flink_tables_file_path):
                with open(flink_tables_file_path, "r") as f:
                    import json

                    config = json.loads(f.read())
                    _, _, connector = backend.get_table_config_and_connector(config, table)
                    if connector and connector["options"]["connector"] == "jdbc":
                        base_url = connector["options"]["url"]
                        username = connector["options"]["username"]
                        password = connector["options"]["password"]
                        split_expr = "://"
                        split_expr_index = base_url.index(split_expr)
                        db_type = base_url[len("jdbc:") : split_expr_index]
                        url = f"{db_type}{split_expr}{username}:{password}@{KV.from_config(base_url, split_expr).v}"
                        return url

        assert isinstance(backend, FlinkBackend)
        db_url = retrieve_jdbc_url_from(flink_tables_file_path, backend, table)
        if db_url:
            from sqlalchemy import create_engine

            engine: Engine = create_engine(db_url, isolation_level="AUTOCOMMIT", pool_size=1)
            conn: Connection = engine.connect()
            return conn


class PostgresBackendProcessor(BackendProcessor):
    def __init__(self, config: EasySqlConfig) -> None:
        super().__init__(config)

    def _create_backend(self, backend_config: Optional[List[str]]) -> Backend:
        assert "PG_URL" in os.environ, "Must set PG_URL env var to run an ETL with postgres backend."
        from easy_sql.sql_processor.backend.rdb import RdbBackend

        return RdbBackend(os.environ["PG_URL"])


class ClickhouseBackendProcessor(BackendProcessor):
    def __init__(self, config: EasySqlConfig) -> None:
        super().__init__(config)

    def _create_backend(self, backend_config: Optional[List[str]]) -> Backend:
        assert "CLICKHOUSE_URL" in os.environ, "Must set CLICKHOUSE_URL env var to run an ETL with Clickhouse backend."
        from easy_sql.sql_processor.backend.rdb import RdbBackend

        return RdbBackend(os.environ["CLICKHOUSE_URL"])


class BigqueryBackendProcessor(BackendProcessor):
    def __init__(self, config: EasySqlConfig) -> None:
        super().__init__(config)

    def _create_backend(self, backend_config: Optional[List[str]]) -> Backend:
        assert (
            "BIGQUERY_CREDENTIAL_FILE" in os.environ
        ), "Must set BIGQUERY_CREDENTIAL_FILE env var to run an ETL with BigQuery backend."
        from easy_sql.sql_processor.backend.rdb import RdbBackend

        return RdbBackend("bigquery://", credentials=os.environ["BIGQUERY_CREDENTIAL_FILE"])

    def _pre_defined_vars(self, backend: Backend) -> Dict:
        return {"temp_db": backend.temp_schema}  # type: ignore


class MaxComputeBackendProcessor(BackendProcessor):
    def __init__(self, config: EasySqlConfig) -> None:
        super().__init__(config)

    def _create_backend(self, backend_config: Optional[List[str]]) -> Backend:
        assert (
            "MAXCOMPUTE_CONFIG_FILE" in os.environ
        ), "Must set MAXCOMPUTE_CONFIG_FILE env var to run an ETL with MaxCompute backend."
        import json

        odps_params = json.loads(os.environ["MAXCOMPUTE_CONFIG_FILE"])
        for k in ["access_id", "secret_access_key", "project", "endpoint"]:
            assert k in odps_params, f"{k} not found in file {os.environ['MAXCOMPUTE_CONFIG_FILE']}"

        from easy_sql.sql_processor.backend.maxcompute import MaxComputeBackend

        return MaxComputeBackend(**odps_params)  # type: ignore
