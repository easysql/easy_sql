from __future__ import annotations

import os
import re
import urllib.parse
from datetime import datetime
from os import path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from easy_sql.sql_processor.backend import FlinkBackend

if TYPE_CHECKING:
    from easy_sql.sql_processor.backend import Backend
    from sqlalchemy.engine.base import Connection, Engine

import click


def resolve_file(file_path: str, abs_path: bool = False) -> str:
    if file_path.lower().startswith("hdfs://"):
        # do not resolve if it is hdfs path
        return file_path
    base_path = os.path.abspath(os.curdir)
    if not path.exists(file_path):
        if path.exists(path.join(base_path, file_path)):
            file_path = path.join(base_path, file_path)
        elif path.exists(path.basename(file_path)):
            file_path = path.basename(file_path)
        else:
            raise Exception(f"file not found: {file_path}")
    if abs_path:
        file_path = path.abspath(file_path)
    if " " in file_path:
        parts = file_path.split("/")
        # Remove space inside file path, since spark will raise issue with this case.
        # We must ensure there is a soft link to the path with space removed to the end.
        file_path = "/".join([re.sub(r" .*$", "", part) for part in parts])
    return file_path


def resolve_files(files_path: str, abs_path: bool = False) -> str:
    return ",".join([resolve_file(f.strip(), abs_path) for f in files_path.split(",") if f.strip()])


def read_sql(sql_file: str):
    with open(resolve_file(sql_file)) as f:
        return f.read()


@click.command(name="data_process")
@click.option("--sql-file", "-f", type=str)
@click.option("--vars", "-v", type=str, required=False)
@click.option("--dry-run", type=str, required=False, help="if dry run, one of [true, 1, false, 0]")
@click.option("--print-command", "-p", is_flag=True)
def data_process(sql_file: str, vars: str, dry_run: str, print_command: bool):
    _data_process(sql_file, vars, dry_run, print_command)


def _data_process(sql_file: str, vars: Optional[str], dry_run: Optional[str], print_command: bool) -> Optional[str]:
    if not sql_file.endswith(".sql"):
        raise Exception(f"sql_file must ends with .sql, found `{sql_file}`")
    dry_run = dry_run if dry_run is not None else "0"

    if print_command:
        command = shell_command(sql_file=sql_file, vars=vars, dry_run=dry_run)
        print(command)
        return command

    is_dry_run = dry_run in ["true", "1"]
    config = EasySqlConfig.from_sql(sql_file)

    from easy_sql.sql_processor import SqlProcessor

    def run_with_vars(backend: Backend, variables: Dict[str, Any]):
        vars_dict: Dict[str, Any] = dict(
            [
                (get_key_by_splitter_and_strip(v), urllib.parse.unquote_plus(get_value_by_splitter_and_strip(v)))
                for v in vars.split(",")
                if v.strip()
            ]
            if vars
            else []
        )
        variables.update(vars_dict)

        sql_processor = SqlProcessor(
            backend, config.sql, variables=variables, scala_udf_initializer=config.scala_udf_initializer
        )
        if config.udf_file_path:
            sql_processor.register_udfs_from_pyfile(
                resolve_file(config.udf_file_path) if "/" in config.udf_file_path else config.udf_file_path
            )
        if config.func_file_path:
            sql_processor.register_funcs_from_pyfile(
                resolve_file(config.func_file_path) if "/" in config.func_file_path else config.func_file_path
            )

        sql_processor.run(dry_run=is_dry_run)

    backend: Backend = create_sql_processor_backend(
        config.backend, config.sql, config.task_name, config.tables, config.customized_easy_sql_conf
    )

    backend_is_bigquery = config.backend == "bigquery"
    pre_defined_vars = {"temp_db": backend.temp_schema if backend_is_bigquery else None}  # type: ignore
    try:
        run_with_vars(backend, pre_defined_vars)
    finally:
        backend.clean()


def create_sql_processor_backend(
    backend_type: str, sql: str, task_name: str, tables: List[str], customized_easy_sql_conf: List[str]
) -> Backend:
    if backend_type == "spark":
        from easy_sql.spark_optimizer import get_spark
        from easy_sql.sql_processor.backend import SparkBackend

        backend = SparkBackend(get_spark(task_name))
        exec_sql = lambda sql: backend.exec_native_sql(sql)  # noqa: E731
    elif backend_type == "flink":
        etl_type = next(
            filter(lambda c: get_key_by_splitter_and_strip(c) == "etl_type", customized_easy_sql_conf), None
        )
        backend = FlinkBackend(get_value_by_splitter_and_strip(etl_type) == "batch" if etl_type else True)

        # just for test
        test_jar_path = "test/flink/jars"
        backend.add_jars(
            [resolve_file(os.path.join(test_jar_path, jar), abs_path=True) for jar in os.listdir(test_jar_path)]
        )

        exec_sql = lambda sql: backend.exec_native_sql(sql)
        flink_tables_file_path = next(
            filter(lambda c: get_key_by_splitter_and_strip(c) == "flink_tables_file_path", customized_easy_sql_conf),
            None,
        )

        if flink_tables_file_path:
            flink_tables_file_path = resolve_file(
                get_value_by_splitter_and_strip(flink_tables_file_path), abs_path=True
            )
            backend.register_tables(flink_tables_file_path, tables)
            if tables:
                conn = get_conn_from(flink_tables_file_path, backend, tables[0])
                if conn:
                    from easy_sql.sql_processor.backend.rdb import _exec_sql

                    exec_sql = lambda sql: _exec_sql(conn, sql)

    elif backend_type == "maxcompute":
        odps_params = {"access_id": "xx", "secret_access_key": "xx", "project": "xx", "endpoint": "xx"}
        from easy_sql.sql_processor.backend.maxcompute import (
            MaxComputeBackend,
            _exec_sql,
        )

        backend = MaxComputeBackend(**odps_params)  # type: ignore
        exec_sql = lambda sql: _exec_sql(backend.conn, sql)  # type: ignore # noqa: E731
    elif backend_type in ["postgres", "clickhouse", "bigquery"]:
        from easy_sql.sql_processor.backend.rdb import RdbBackend, _exec_sql

        if backend_type == "postgres":
            assert "PG_URL" in os.environ, "Must set PG_URL env var to run an ETL with postgres backend."
            backend = RdbBackend(os.environ["PG_URL"])
        elif backend_type == "clickhouse":
            assert (
                "CLICKHOUSE_URL" in os.environ
            ), "Must set CLICKHOUSE_URL env var to run an ETL with Clickhouse backend."
            backend = RdbBackend(os.environ["CLICKHOUSE_URL"])
        elif backend_type == "bigquery":
            assert (
                "BIGQUERY_CREDENTIAL_FILE" in os.environ
            ), "Must set BIGQUERY_CREDENTIAL_FILE env var to run an ETL with BigQuery backend."
            backend = RdbBackend("bigquery://", credentials=os.environ["BIGQUERY_CREDENTIAL_FILE"])
        else:
            raise Exception(f"unsupported backend: {backend_type}")
        exec_sql = lambda sql: _exec_sql(backend.conn, sql)  # noqa: E731
    else:
        raise Exception("Unsupported backend found: " + backend_type)

    prepare_sql_list = []
    for line in sql.split("\n"):
        if re.match(r"^-- \s*prepare-sql:.*$", line):
            prepare_sql_list.append(get_value_by_splitter_and_strip(line, "prepare-sql:"))
    for prepare_sql in prepare_sql_list:
        exec_sql(prepare_sql)

    return backend


class EasySqlConfig:
    def __init__(
        self,
        sql_file: Optional[str],
        sql: str,
        backend: str,
        customized_backend_conf: List[str],
        customized_easy_sql_conf: List[str],
        udf_file_path: Optional[str],
        func_file_path: Optional[str],
        scala_udf_initializer: Optional[str],
        tables: List[str],
    ):
        self.sql_file = sql_file
        self.sql = sql
        self.backend = backend
        self.customized_backend_conf, self.customized_easy_sql_conf = customized_backend_conf, customized_easy_sql_conf
        self.udf_file_path, self.func_file_path = udf_file_path, func_file_path
        self.scala_udf_initializer = scala_udf_initializer
        self.tables = tables

    @staticmethod
    def from_sql(sql_file: Optional[str] = None, sql: Optional[str] = None) -> EasySqlConfig:  # type: ignore
        assert sql_file is not None or sql is not None, "sql_file or sql must be set"
        sql: str = read_sql(sql_file) if sql_file else sql  # type: ignore
        sql_lines = sql.split("\n")  # type: ignore

        backend = _parse_backend(sql)
        tables = _parse_tables(sql)

        customized_backend_conf: List[str] = []
        customized_easy_sql_conf: List[str] = []
        for line in sql_lines:
            if re.match(r"^-- \s*config:.*$", line):
                config_value = get_value_by_splitter_and_strip(line, "config:")
                if config_value.strip().lower().startswith("easy_sql."):
                    customized_easy_sql_conf += [get_value_by_splitter_and_strip(config_value, "easy_sql.")]
                else:
                    customized_backend_conf += [config_value]

        udf_file_path, func_file_path, scala_udf_initializer = None, None, None
        for c in customized_easy_sql_conf:
            if c.startswith("udf_file_path"):
                udf_file_path = get_value_by_splitter_and_strip(c)
            elif c.startswith("func_file_path"):
                func_file_path = get_value_by_splitter_and_strip(c)
            elif c.startswith("scala_udf_initializer"):
                scala_udf_initializer = get_value_by_splitter_and_strip(c)
        return EasySqlConfig(
            sql_file,
            sql,
            backend,
            customized_backend_conf,
            customized_easy_sql_conf,
            udf_file_path,
            func_file_path,
            scala_udf_initializer,
            tables,
        )

    @property
    def spark_submit(self):
        # 默认情况下使用系统中默认spark版本下的spark-submit
        # sql代码中指定了easy_sql.spark_submit时，优先级高于default配置
        spark_submit = "spark-submit"
        for c in self.customized_easy_sql_conf:
            if c.startswith("spark_submit"):
                spark_submit = get_value_by_splitter_and_strip(c)
        return spark_submit

    @property
    def flink(self):
        # 默认情况下使用系统中默认flink版本下的flink
        # sql代码中指定了easy_sql.flink时，优先级高于default配置
        flink = "flink"
        for c in self.customized_easy_sql_conf:
            if c.startswith("flink_run"):
                flink = get_value_by_splitter_and_strip(c)
        return flink

    @property
    def task_name(self):
        assert self.sql_file is not None
        sql_name = path.basename(self.sql_file)[:-4]
        return f'{sql_name}_{datetime.now().strftime("%Y%m%d%H%M%S")}'

    def spark_conf_command_args(self) -> List[str]:
        # config 的优先级：1. sql 代码里的 config 优先级高于这里的 default 配置
        # 对于数组类的 config，sql 代码里的 config 会添加进来，而不是覆盖默认配置
        assert self.sql_file is not None
        default_conf = [
            "spark.master=local[2]",
            "spark.submit.deployMode=client",
            f"spark.app.name={self.task_name}",
            "spark.sql.warehouse.dir=/tmp/spark-warehouse-localdw",
            'spark.driver.extraJavaOptions="-Dderby.system.home=/tmp/spark-warehouse-metastore'
            ' -Dderby.stream.error.file=/tmp/spark-warehouse-metastore.log"',
            f'spark.files="{resolve_file(self.sql_file, abs_path=True)}'
            f'{"," + resolve_file(self.udf_file_path, abs_path=True) if self.udf_file_path else ""}'
            f'{"," + resolve_file(self.func_file_path, abs_path=True) if self.func_file_path else ""}'
            '"',
        ]
        args = self.build_conf_command_args(default_conf, ["spark.files", "spark.jars", "spark.submit.pyFiles"])
        return [f"--conf {arg}={args[arg]}" for arg in args]

    def flink_conf_command_args(self) -> List[str]:
        # config 的优先级：1. sql 代码里的 config 优先级高于这里的 default 配置
        # 对于数组类的 config，sql 代码里的 config 会添加进来，而不是覆盖默认配置
        assert self.sql_file is not None
        default_conf = [
            "parallelism=1",
            (
                f"pyFiles={resolve_file(self.sql_file, abs_path=True)}"
                f'{"," + resolve_file(self.udf_file_path, abs_path=True) if self.udf_file_path else ""}'
                f'{"," + resolve_file(self.func_file_path, abs_path=True) if self.func_file_path else ""}'
            ),
        ]
        args = self.build_conf_command_args(default_conf, ["pyFiles"])
        return [f"--{arg} {args[arg]}" for arg in args]

    def build_conf_command_args(self, default_conf: List[str], merge_keys: List[str]) -> dict[str, str]:
        # config 的优先级：1. sql 代码里的 config 优先级高于这里的 default 配置
        # 对于数组类的 config，sql 代码里的 config 会添加进来，而不是覆盖默认配置
        assert self.sql_file is not None
        customized_conf_keys = [get_key_by_splitter_and_strip(c) for c in self.customized_backend_conf]
        customized_backend_conf = self.customized_backend_conf.copy()

        args: dict[str, str] = {}
        for conf in default_conf:
            conf_key = get_key_by_splitter_and_strip(conf)
            if conf_key not in customized_conf_keys:
                args.update({conf_key: get_value_by_splitter_and_strip(conf)})
            else:
                customized_conf = [conf for conf in customized_backend_conf if conf.startswith(conf_key)][0]
                if conf_key in merge_keys:
                    customized_values = [
                        resolve_file(val.strip(), abs_path=True)
                        for val in get_value_by_splitter_and_strip(customized_conf, "=", '"').split(",")
                        if val.strip()
                    ]
                    default_values = [v for v in get_value_by_splitter_and_strip(conf, "=", '"').split(",") if v]
                    args.update({conf_key: f'"{",".join(set(default_values + customized_values))}"'})
                else:
                    args.update({conf_key: get_value_by_splitter_and_strip(customized_conf)})
                customized_backend_conf.remove(customized_conf)
        for conf in customized_backend_conf:
            args.update({get_key_by_splitter_and_strip(conf): get_value_by_splitter_and_strip(conf)})
        return args


def shell_command(sql_file: str, vars: Optional[str], dry_run: str):
    config = EasySqlConfig.from_sql(sql_file)

    if config.backend == "spark":
        return (
            f'{config.spark_submit} {" ".join(config.spark_conf_command_args())} "{path.abspath(__file__)}" '
            f"-f {sql_file} --dry-run {dry_run} "
            f'{"-v " + vars if vars else ""} '
        )
    elif config.backend == "flink":
        return (
            f'{config.flink} run {" ".join(config.flink_conf_command_args())} '
            f'--python "{path.abspath(__file__)}" '
            f"-f {sql_file} --dry-run {dry_run} "
            f'{"-v " + vars if vars else ""} '
        )
    else:
        raise Exception("No need to print shell command for non-spark backends")


def _parse_backend(sql: str):
    sql_lines = sql.split("\n")

    backend = "spark"
    for line in sql_lines:
        if re.match(r"^-- \s*backend:.*$", line):
            backend = get_value_by_splitter_and_strip(line, "backend:").split(" ")[0]
            break
    supported_backends = ["spark", "postgres", "clickhouse", "maxcompute", "bigquery", "flink"]
    if backend not in supported_backends:
        raise Exception(f"unsupported backend `${backend}`, all supported backends are: {supported_backends}")
    return backend


def _parse_tables(sql: str):
    sql_lines = sql.split("\n")
    INPUTS = "inputs:"
    OUTPUTS = "outputs:"
    tables = []
    for line in sql_lines:
        if re.match(rf"^-- \s*{INPUTS}.*$", line):
            tables += get_value_by_splitter_and_strip(line, INPUTS).split(",")
        elif re.match(rf"^-- \s*{OUTPUTS}.*$", line):
            tables += get_value_by_splitter_and_strip(line, OUTPUTS).split(",")
    return list({t.strip() for t in tables})


def get_conn_from(flink_tables_file_path: str, backend: FlinkBackend, table: str):
    db_url = retrieve_jdbc_url_from(flink_tables_file_path, backend, table)
    if db_url:
        from sqlalchemy import create_engine

        engine: Engine = create_engine(db_url, isolation_level="AUTOCOMMIT", pool_size=1)
        conn: Connection = engine.connect()
        return conn


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
                url = (
                    f"{db_type}{split_expr}{username}:{password}@{get_value_by_splitter_and_strip(base_url, split_expr)}"
                )
                return url


def get_key_by_splitter_and_strip(source: str, splitter: Optional[str] = "=", strip: Optional[str] = None):
    return source.strip()[: source.strip().index(splitter or "=")].strip(strip)


def get_value_by_splitter_and_strip(source: str, splitter: Optional[str] = "=", strip: Optional[str] = None):
    splitter = splitter or "="
    return source.strip()[source.strip().index(splitter) + len(splitter) :].strip(strip)


if __name__ == "__main__":
    # test cases:
    # print(shell_command('source/dm/dm_source.sales_count.spark.sql', '20210505', 'day', '1', 'test-task', 'test-task', None, None, None)) # noqa: B950
    # print(shell_command('sales/dm/indicator/sql/dm_sales.order_count.sql', '20210505', 'day', '1', 'test-task', 'test-task',
    #                     None, None, None, None, None, '1').replace('--conf', '\n --conf'))
    # print(shell_command('sales/samples/dm_source.sales_count.ch.sql', '20210505', 'day', '1', 'test-task', 'test-task',
    #                     None, None, None, None, '1').replace('--conf', '\n --conf'))
    # print(shell_command('sales/samples/aws_glue_sample.sql', '20210505', 'day', '1', 'test-task', 'test-task', None,
    #                     'sales/etl_generator.py', 'sales/samples/aws_funcs.py', '', '1').replace('--conf', '\n --conf'))
    # print(shell_command('sales/samples/dm_source.sales_count.mc.sql', '20220301', 'day', '1', 'test-task', 'test-task',
    #                     None, None, None, None, '1').replace('--conf', '\n --conf'))
    data_process()
