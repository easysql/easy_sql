from __future__ import annotations

import os
import re
import urllib.parse
from datetime import datetime
from os import path
from typing import Any, Dict, List, Optional

from easy_sql.logger import logger
from easy_sql.utils.io_utils import read_sql, resolve_file
from easy_sql.utils.kv import (
    KV,
    get_key_by_splitter_and_strip,
    get_value_by_splitter_and_strip,
)


def parse_backend(sql: str):
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


def _parse_tables(sql: str, table_type: str) -> List[str]:
    sql_lines = sql.split("\n")
    tables = []
    for line in sql_lines:
        if re.match(rf"^-- \s*{table_type}:.*$", line):
            tables += get_value_by_splitter_and_strip(line, table_type + ":").split(",")
    return tables


def parse_vars(vars: Optional[str]) -> Dict[str, Any]:
    vars_dict: Dict[str, Any] = dict(
        [
            (get_key_by_splitter_and_strip(v), urllib.parse.unquote_plus(get_value_by_splitter_and_strip(v)))
            for v in vars.split(",")
            if v.strip()
        ]
        if vars
        else []
    )
    return vars_dict


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
        input_tables: List[str],
        output_tables: List[str],
    ):
        self.sql_file = sql_file
        self.sql = sql
        self.backend = backend
        self.customized_backend_conf, self.customized_easy_sql_conf = customized_backend_conf, customized_easy_sql_conf
        self.udf_file_path, self.func_file_path = udf_file_path, func_file_path
        self.scala_udf_initializer = scala_udf_initializer
        self.input_tables, self.output_tables = input_tables, output_tables
        self.resolved_udf_file_path = self._resolve_file(udf_file_path) if udf_file_path else None
        self.resolved_func_file_path = self._resolve_file(func_file_path) if func_file_path else None

    @staticmethod
    def from_sql(
        sql_file: Optional[str] = None, sql: Optional[str] = None, system_config_prefix: str = "easy_sql."  # type: ignore
    ) -> EasySqlConfig:
        assert sql_file is not None or sql is not None, "sql_file or sql must be set"
        if sql and sql_file:
            logger.info("both sql and sql_file set, will use sql as file content")
        sql: str = sql if sql else read_sql(sql_file)  # type: ignore
        sql_lines = sql.split("\n")  # type: ignore

        backend = parse_backend(sql)
        input_tables, output_tables = _parse_tables(sql, "inputs"), _parse_tables(sql, "outputs")

        customized_backend_conf: List[str] = []
        customized_easy_sql_conf: List[str] = []
        for line in sql_lines:
            if re.match(r"^-- \s*config:.*$", line):
                config_value = get_value_by_splitter_and_strip(line, "config:")
                if config_value.strip().lower().startswith(system_config_prefix):
                    customized_easy_sql_conf += [get_value_by_splitter_and_strip(config_value, system_config_prefix)]
                else:
                    customized_backend_conf += [config_value]

        udf_file_path, func_file_path, scala_udf_initializer = None, None, None
        for c in customized_easy_sql_conf:
            if c.startswith("udf_file_path"):
                udf_file_path = get_value_by_splitter_and_strip(c)
                udf_file_path = resolve_file(udf_file_path) if udf_file_path and "/" in udf_file_path else udf_file_path
            elif c.startswith("func_file_path"):
                func_file_path = get_value_by_splitter_and_strip(c)
                func_file_path = (
                    resolve_file(func_file_path) if func_file_path and "/" in func_file_path else func_file_path
                )
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
            input_tables,
            output_tables,
        )

    @property
    def tables(self) -> List[str]:
        return list({t.strip() for t in self.input_tables + self.output_tables})

    @property
    def is_batch(self) -> bool:
        etl_type_config = next(
            filter(lambda c: get_key_by_splitter_and_strip(c) == "etl_type", self.customized_easy_sql_conf), None
        )
        return get_value_by_splitter_and_strip(etl_type_config) == "batch" if etl_type_config else True

    @property
    def is_streaming(self) -> bool:
        return not self.is_batch

    @property
    def task_name(self):
        assert self.sql_file is not None
        sql_name = path.basename(self.sql_file)[:-4]
        return f'{sql_name}_{datetime.now().strftime("%Y%m%d%H%M%S")}'

    @property
    def prepare_sql_list(self) -> List[str]:
        prepare_sql_list = []
        for line in self.sql.split("\n"):
            if re.match(r"^-- \s*prepare-sql:.*$", line):
                prepare_sql_list.append(get_value_by_splitter_and_strip(line, "prepare-sql:"))
        return prepare_sql_list

    @property
    def abs_sql_file_path(self) -> str:
        assert self.sql_file is not None
        return resolve_file(self.sql_file, abs_path=True)

    def _resolve_file(self, file_path: str, *, prefix: str = "") -> str:
        return resolve_file(file_path, abs_path=True, prefix=prefix, relative_to=self.abs_sql_file_path)

    def _build_conf_command_args(
        self,
        default_conf: List[str],
        user_default_conf: List[str],
        file_keys: List[str],
        customized_confs: List[str],
        prefix: str = "",
    ) -> dict[str, str]:
        # config 的优先级：
        # 1. sql 代码里的 config 优先级高于用户定义的 default 配置
        # 2. 用户定义的 default 配置高于Easy SQL定义的 default 配置
        # 3. 对于文件类的 config，sql 代码里的 config 会添加进来，而不是覆盖默认配置
        assert self.sql_file is not None
        customized_confs = customized_confs.copy()

        files = lambda files_str: [f.strip() for f in files_str.strip('"').split(",") if f.strip()]
        resolved_files = lambda _files, prefix: [self._resolve_file(f.strip(), prefix=prefix) for f in files(_files)]

        args: dict[str, str] = {}
        for conf in default_conf:
            args.update(KV.from_config(conf).as_dict())
        for prioritized_confs in [user_default_conf, customized_confs]:
            for conf in prioritized_confs:
                kv = KV.from_config(conf)
                if kv.k in file_keys:
                    existing_files = files(args.get(kv.k, ""))
                    args.update({kv.k: f'"{",".join(set(existing_files + resolved_files(kv.v, prefix)))}"'})
                else:
                    args.update({get_key_by_splitter_and_strip(conf): get_value_by_splitter_and_strip(conf)})
        return args


class SparkBackendConfig:
    def __init__(self, config: EasySqlConfig, default_config: Optional[List[str]] = None) -> None:
        self.config = config
        self.user_default_conf = default_config

    @property
    def spark_submit(self):
        # 默认情况下使用系统中默认spark版本下的spark-submit
        # sql代码中指定了easy_sql.spark_submit时，优先级高于default配置
        spark_submit = "spark-submit"
        for c in self.config.customized_easy_sql_conf:
            if c.startswith("spark_submit"):
                spark_submit = get_value_by_splitter_and_strip(c)
        return spark_submit

    def spark_conf_command_args(self) -> List[str]:
        # config 的优先级：1. sql 代码里的 config 优先级高于这里的 default 配置
        # 对于数组类的 config，sql 代码里的 config 会添加进来，而不是覆盖默认配置
        config = self.config
        assert config.sql_file is not None
        sys_default_conf = [
            "spark.master=local[2]",
            "spark.submit.deployMode=client",
            f"spark.app.name={self.config.task_name}",
            "spark.sql.warehouse.dir=/tmp/spark-warehouse-localdw",
            (
                'spark.driver.extraJavaOptions="-Dderby.system.home=/tmp/spark-warehouse-metastore'
                ' -Dderby.stream.error.file=/tmp/spark-warehouse-metastore.log"'
            ),
            (
                f'spark.files="{self.config.abs_sql_file_path}'
                f'{"," + self.config._resolve_file(config.udf_file_path) if config.udf_file_path else ""}'
                f'{"," + self.config._resolve_file(config.func_file_path) if config.func_file_path else ""}'
                '"'
            ),
        ]
        args = config._build_conf_command_args(
            sys_default_conf,
            self.user_default_conf or [],
            ["spark.files", "spark.jars", "spark.submit.pyFiles"],
            config.customized_backend_conf,
        )
        return [f"--conf {arg}={args[arg]}" for arg in args]


class FlinkBackendConfig:
    def __init__(self, config: EasySqlConfig, default_config: Optional[List[str]] = None) -> None:
        self.config = config
        self.user_default_conf = default_config

    @property
    def flink_configurations(self) -> Dict[str, str]:
        # refer: https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/python_config/
        # refer: https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/config/
        configs = dict(
            [
                KV.from_config(config).as_tuple()
                for config in self.user_default_conf or []
                if config and not config.startswith("-")
            ]
        )
        configs.update(
            {
                get_value_by_splitter_and_strip(
                    get_key_by_splitter_and_strip(arg), "flink."
                ): get_value_by_splitter_and_strip(arg)
                for arg in self.config.customized_backend_conf
                if arg.startswith("flink.") and not arg.startswith("flink.cmd")
            }
        )

        file_keys = ["python.archives", "python.files", "python.requirements", "pipeline.jars"]
        for k in file_keys:
            if k in configs:
                splitter = "," if k.startswith("python.") else ";"
                configs[k] = splitter.join({self._resolve_file(f) for f in configs[k].split(splitter) if f.strip()})
                configs[k] = f"{configs[k]}"

        return configs

    @property
    def flink(self):
        # 默认情况下使用系统中默认flink版本下的flink
        # sql代码中指定了easy_sql.flink时，优先级高于default配置
        try:
            from pyflink import find_flink_home

            flink = os.path.join(find_flink_home._find_flink_home(), "bin/flink")
        except Exception:
            flink = "flink"
        for c in self.config.customized_easy_sql_conf:
            if c.startswith("flink_run"):
                flink = get_value_by_splitter_and_strip(c)
        return flink

    @property
    def flink_tables_file_path(self) -> Optional[str]:
        flink_tables_file_path = next(
            filter(
                lambda c: get_key_by_splitter_and_strip(c) == "flink_tables_file_path",
                self.config.customized_easy_sql_conf,
            ),
            None,
        )
        if flink_tables_file_path is None:
            return None
        else:
            return self.config._resolve_file(get_value_by_splitter_and_strip(flink_tables_file_path))

    def _resolve_file(self, file_path: str) -> str:
        return self.config._resolve_file(file_path, prefix="file://")

    def flink_conf_command_args(self) -> List[str]:
        # config 的优先级：1. sql 代码里的 config 优先级高于这里的 default 配置
        # 对于数组类的 config，sql 代码里的 config 会添加进来，而不是覆盖默认配置
        config = self.config
        default_conf = [
            "--parallelism=1",
            (
                f"--pyFiles={'file://' + self.config.abs_sql_file_path}"
                f'{"," + self._resolve_file(config.udf_file_path) if config.udf_file_path else ""}'
                f'{"," + self._resolve_file(config.func_file_path) if config.func_file_path else ""}'
            ),
        ]

        # refer: https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/cli/
        file_keys = [
            "-pyarch",
            "--pyArchives",
            "-pyfs",
            "--pyFiles",
        ]
        cmd_value_args = []
        for c in config.customized_backend_conf:
            if get_key_by_splitter_and_strip(c) == "flink.cmd":
                key = get_key_by_splitter_and_strip(get_value_by_splitter_and_strip(c), " ")
                value = get_value_by_splitter_and_strip(get_value_by_splitter_and_strip(c), " ")
                if key in file_keys:
                    resolve_files = [f"{self._resolve_file(val.strip())}" for val in value.split(",") if val.strip()]
                    value = f'"{",".join(set(resolve_files))}"'
                cmd_value_args.append(f"{key}={value}")

        args = config._build_conf_command_args(
            default_conf, self.user_default_conf or [], file_keys, cmd_value_args, prefix="file://"
        )
        cmd_args = [f"{arg} {args[arg]}" for arg in args]

        return cmd_args
