from __future__ import annotations

import os
import re
import sys
from typing import TYPE_CHECKING

import click

if TYPE_CHECKING:
    from easy_sql.sql_processor.backend.base import Backend

from .sql_tester import SqlTester, TestCase, WorkPath


@click.group()
def main():
    pass


def create_backend(backend_type: str, env: str, case: TestCase, work_path: WorkPath) -> Backend:
    if backend_type == "spark":
        from easy_sql.local_spark import LocalSpark
        from easy_sql.sql_processor.backend import SparkBackend

        custom_conf = {
            "spark.sql.warehouse.dir": f"/tmp/spark-warehouse-{env}",
            "spark.driver.extraJavaOptions": (
                f"-Dderby.system.home=/tmp/spark-metastore-{env} "
                f"-Dderby.stream.error.file=/tmp/spark-metastore-{env}.log"
            ),
        }
        if case.udf_file_paths or case.func_file_paths:
            paths = case.udf_file_paths + case.func_file_paths
            custom_conf["spark.files"] = ",".join([work_path.path(path) for path in paths])
            # need to recreate SparkSession when there's new config
            LocalSpark.stop()

        import shutil

        shutil.rmtree(f"/tmp/spark-warehouse-{env}", True)

        # try get a clean spark environment
        spark = LocalSpark.get(custom_conf)
        backend = SparkBackend(spark)
        backend.clean()
        return backend
    elif backend_type == "postgres":
        from easy_sql.sql_processor.backend.rdb import RdbBackend

        assert "PG_URL" in os.environ, "Must set PG_URL env var to run an ETL with postgres backend."
        pg_url = os.environ["PG_URL"]
        backend = RdbBackend(pg_url)

        i = 0
        while True:
            try:
                backend.exec_native_sql(f"drop database if exists unittest{i}")
                break
            except Exception as e:
                print(e.args)
                if re.match(r".*psycopg2.errors.ObjectInUse.*", e.args[0]):
                    i += 1
                else:
                    raise e

        backend.exec_native_sql(f"create database unittest{i}")
        backend = RdbBackend(pg_url[: pg_url.rindex("/")] + f"/unittest{i}")
        return backend
    elif backend_type == "clickhouse":
        from easy_sql.sql_processor.backend.rdb import RdbBackend

        assert "CLICKHOUSE_URL" in os.environ, "Must set CLICKHOUSE_URL env var to run an ETL with clickhouse backend."
        ch_url = os.environ["CLICKHOUSE_URL"]
        backend = RdbBackend(ch_url)
        return backend
    elif backend_type == "bigquery":
        from easy_sql.sql_processor.backend.rdb import RdbBackend

        backend = RdbBackend(
            "bigquery://", credentials=f"{os.environ.get('HOME', '/tmp')}/.bigquery/credential-test.json"
        )
        return backend
    else:
        raise Exception("unsupported backend: " + backend_type)


_FILES_ARG_HELP = "Test files (multiple files split by comma): file1,file2,..."
_BACKEND_ARG_HELP = (
    "Which backend to use. The supported backends are: spark postgres bigquery clickhouse.\n"
    "For spark backend, the temporary folder will be /tmp/spark-metastore-{ENV} .\n"
    "For postgres backend, an environment variable named PG_URL must be set.\n"
    "For clickhouse backend, an environment variable named CLICKHOUSE_URL must be set.\n"
    "For bigquery backend, the credential file must be put at ${HOME}/.bigquery/credential-{ENV}.json .\n"
)
_ENV_ARG_HELP = (
    "Environment identifier to indicate which env to run on. Possible values could be: test e2e\n"
    "Used for logging and temporary file or folder creation."
)


@main.command(name="run-test")
@click.option("--files", "-f", type=str, help=_FILES_ARG_HELP)
@click.option("--backend", "-b", type=str, help=_BACKEND_ARG_HELP, required=False, default="spark", show_default=True)
@click.option("--env", "-e", type=str, help=_ENV_ARG_HELP, required=False, default="test", show_default=True)
def run_test(files, backend: str = "spark", env: str = "test"):
    _run_test(files, backend, env)


def _run_test(files, backend: str = "spark", env: str = "test"):
    test_data_files = [os.path.abspath(f.strip()) for f in files.split(",") if f.strip()]
    for f in test_data_files:
        if not os.path.exists(f):
            print(f"[ERROR] File does not exist: `{sys.argv}`")
            sys.exit(1)

    work_dir = os.path.abspath(os.curdir)
    SqlTester(
        env=env, backend_creator=lambda case: create_backend(backend, env, case, WorkPath(work_dir)), work_dir=work_dir
    ).run_tests(test_data_files)


@main.command(name="convert-json")
@click.option("--files", "-f", type=str, help=_FILES_ARG_HELP)
def convert_json(files):
    _convert_json(files)


def _convert_json(files):
    test_data_files = [os.path.abspath(f.strip()) for f in files.split(",") if f.strip()]
    sql_tester = SqlTester(None, work_dir=os.path.abspath(os.curdir))  # type: ignore
    for f in test_data_files:
        sql_tester.convert_cases_to_json(f)


if __name__ == "__main__":
    main()
