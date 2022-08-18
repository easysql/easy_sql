import unittest

from easy_sql import base_test
from easy_sql.base_test import (
    TEST_BQ_URL,
    TEST_CH_URL,
    TEST_PG_URL,
    bigquery_sql_expr,
    date,
    dt,
    dt_zone,
)


class SqlProcessorTest(unittest.TestCase):
    def test_should_run_sql_for_pg_backend(self):
        from easy_sql.sql_processor.backend.rdb import RdbBackend, _exec_sql

        backend = RdbBackend(TEST_PG_URL)
        _exec_sql(backend.conn, "drop schema if exists t cascade")
        _exec_sql(backend.conn, "create schema t")
        self.run_sql_for_pg_backend(backend)

    def test_should_run_sql_for_ch_backend(self):
        from easy_sql.sql_processor.backend.rdb import RdbBackend, _exec_sql

        backend = RdbBackend(TEST_CH_URL)
        _exec_sql(backend.conn, "drop database if exists t")
        _exec_sql(backend.conn, "create database t")
        self.run_sql_for_pg_backend(backend)

    def test_should_run_sql_for_bq_backend(self):
        if not base_test.should_run_integration_test("bq"):
            return
        import os

        from easy_sql.sql_processor.backend.rdb import RdbBackend, _exec_sql

        backend = RdbBackend(
            TEST_BQ_URL,
            credentials=f"{os.environ.get('HOME', '/tmp')}/.bigquery/credential-test.json",
            sql_expr=bigquery_sql_expr,
        )
        _exec_sql(backend.conn, "drop schema if exists t cascade")
        _exec_sql(backend.conn, "create schema if not exists t")
        self.run_sql_for_pg_backend(backend)

    def test_should_run_sql_for_ch_backend_for_dynamic_partitions(self):
        from easy_sql.sql_processor.backend.rdb import RdbBackend, _exec_sql

        backend = RdbBackend(TEST_CH_URL)
        _exec_sql(backend.conn, "drop database if exists t")
        _exec_sql(backend.conn, "create database t")
        self.run_sql_for_dynamic_partitions(backend)

    def test_should_run_sql_for_pg_backend_for_dynamic_partitions(self):
        from easy_sql.sql_processor.backend.rdb import RdbBackend, _exec_sql

        backend = RdbBackend(TEST_PG_URL)
        _exec_sql(backend.conn, "drop schema if exists t cascade")
        _exec_sql(backend.conn, "create schema t")
        self.run_sql_for_dynamic_partitions(backend)

    def test_should_run_sql_for_bq_backend_for_dynamic_partitions(self):
        if not base_test.should_run_integration_test("bq"):
            return
        import os

        from easy_sql.sql_processor.backend.rdb import RdbBackend, _exec_sql

        backend = RdbBackend(
            TEST_BQ_URL,
            credentials=f"{os.environ.get('HOME', '/tmp')}/.bigquery/credential-test.json",
            sql_expr=bigquery_sql_expr,
        )
        _exec_sql(backend.conn, "drop schema if exists t cascade")
        _exec_sql(backend.conn, "create schema t")
        self.run_sql_for_dynamic_partitions(backend)

    def run_sql_for_dynamic_partitions(self, backend):
        from easy_sql.sql_processor import SqlProcessor

        prefix = "${temp_db}." if backend.is_bigquery_backend else ""
        sql = f"""
        -- target=variables
        select '2021-01-01'       as __partition__data_date
        -- target=temp.result
        select 1 as a
        -- target=output.t.result
        select *, 2 as b from {prefix}result
        """
        processor = SqlProcessor(
            backend,
            sql,
            variables={
                "__create_output_table__": True,
                "temp_db": backend.temp_schema if backend.is_bigquery_backend else None,
            },
        )
        processor.func_runner.register_funcs({"t": lambda a, b: int(a) + int(b)})
        processor.run(dry_run=False)
        sql = f"""
        -- target=temp.result1
        select 1 as a
        -- target=output.t.result
        select *, 3 as b, '2021-01-01' as data_date from {prefix}result1 union all
        select *, 3 as b, '2021-01-02' as data_date from {prefix}result1
        -- target=output.t.result
        select *, 3 as b, '2021-01-03' as data_date from {prefix}result1
        """
        processor = SqlProcessor(
            backend, sql, variables={"temp_db": backend.temp_schema if backend.is_bigquery_backend else None}
        )
        processor.func_runner.register_funcs({"t": lambda a, b: int(a) + int(b)})
        processor.run(dry_run=False)

        if backend.is_bigquery_backend:
            mock_dt_1, mock_dt_2, mock_dt_3 = date("2021-01-01"), date("2021-01-02"), date("2021-01-03")
        else:
            mock_dt_1, mock_dt_2, mock_dt_3 = "2021-01-01", "2021-01-02", "2021-01-03"
        self.assertEqual(
            backend.exec_sql("select * from t.result order by data_date").collect(),
            [(1, 3, mock_dt_1), (1, 3, mock_dt_2), (1, 3, mock_dt_3)],
        )

    def run_sql_for_pg_backend(self, backend):
        from easy_sql.sql_processor import SqlProcessor

        prefix = "${temp_db}." if backend.is_bigquery_backend else ""
        sql = f"""
        -- backend: postgres
        -- target=variables
        select
            1                    as __create_output_table__
            , 'append'           as __save_mode__
            , '2021-01-01'       as __partition__data_date
        -- target=temp.result
        select 1 as a
        -- target=temp.result1
        select *, 2 as b from {prefix}result
        -- target=output.t.result
        select *, 2 as b from {prefix}result
        -- target=output.t.result
        select * from {prefix}result1
        -- target=output.t.result1
        select 1 as a, 2 as b, 't' as c, cast('2021-01-01' as timestamp) as d, ${{t(1, 2)}} as e
        -- target=output.t.result1
        select 1 as a, 2 as b, 't' as c, cast('2021-01-01' as timestamp) as d, ${{t(1, 2)}} as e
        """
        processor = SqlProcessor(
            backend,
            sql,
            variables={
                "__create_output_table__": True,
                "temp_db": backend.temp_schema if backend.is_bigquery_backend else None,
            },
        )
        processor.func_runner.register_funcs({"t": lambda a, b: int(a) + int(b)})
        processor.run(dry_run=False)

        if backend.is_bigquery_backend:
            from datetime import timedelta, timezone

            mock_dt, mock_datetime = date("2021-01-01"), dt_zone(
                "2021-01-01 00:00:00", timezone=timezone(timedelta(hours=0))
            )
        else:
            mock_dt, mock_datetime = "2021-01-01", dt("2021-01-01 00:00:00")

        self.assertEqual(backend.exec_sql("select * from t.result").collect(), [(1, 2, mock_dt), (1, 2, mock_dt)])
        self.assertEqual(
            backend.exec_sql("select * from t.result1").collect(),
            [(1, 2, "t", mock_datetime, 3, mock_dt), (1, 2, "t", mock_datetime, 3, mock_dt)],
        )
