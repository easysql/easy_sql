from __future__ import annotations

import re
import time
import traceback
import unittest
from typing import TYPE_CHECKING

from easy_sql import base_test
from easy_sql.base_test import (
    TEST_BQ_URL,
    TEST_CH_URL,
    TEST_PG_URL,
    bigquery_sql_expr,
    date,
    dt,
)
from easy_sql.sql_processor.backend import Partition, SaveMode, TableMeta
from easy_sql.sql_processor.backend.rdb import RdbBackend, RdbRow, TimeLog, _exec_sql

if TYPE_CHECKING:
    from sqlalchemy.engine.reflection import Inspector


class RdbTest(unittest.TestCase):
    def test_log_time(self):
        with TimeLog("start", "end({time_took:.3f})"):
            time.sleep(0.1)
        try:
            with TimeLog("start", "end({time_took:.3f})"):
                raise Exception("some exception")
        except Exception:
            print("printing exception in user code:")
            traceback.print_exc()

    def test_clean_pg_temp_schema(self):
        pg = RdbBackend(TEST_PG_URL)
        from sqlalchemy import inspect

        insp: Inspector = inspect(pg.engine)
        temp_schemas = insp.get_schema_names()
        for schema in temp_schemas:
            if schema.startswith("sp_temp_"):
                _exec_sql(pg.conn, f"drop schema if exists {schema} cascade")

    def test_clean_ch_temp_schema(self):
        backend = RdbBackend(TEST_CH_URL)
        from sqlalchemy import inspect

        insp: Inspector = inspect(backend.engine)
        temp_schemas = insp.get_schema_names()
        for schema in temp_schemas:
            if schema.startswith("sp_temp_"):
                _exec_sql(backend.conn, f"drop database if exists {schema}")

    def test_clean_bq_temp_schema(self):
        if not base_test.should_run_integration_test("bq"):
            return
        import os

        backend = RdbBackend(
            TEST_BQ_URL,
            credentials=f"{os.environ.get('HOME', '/tmp')}/.bigquery/credential-prod.json",
            sql_expr=bigquery_sql_expr,
        )
        from sqlalchemy import inspect

        insp: Inspector = inspect(backend.engine)
        temp_schemas = insp.get_schema_names()
        for schema in temp_schemas:
            if schema.startswith("sp_temp_"):
                _exec_sql(backend.conn, f"drop schema if exists {schema} cascade")

    def test_pg_table(self):
        backend = RdbBackend(TEST_PG_URL)
        _exec_sql(backend.conn, "drop schema if exists t cascade")
        _exec_sql(backend.conn, "create schema t")
        _exec_sql(backend.conn, "drop schema if exists t1 cascade")
        _exec_sql(backend.conn, "create schema t1")
        _exec_sql(backend.conn, "create table t.test(id int, val varchar(100))")
        _exec_sql(backend.conn, "insert into t.test values(1, '1'), (2, '2'), (3, '3')")
        self.run_test_table(backend)

    def test_ch_table(self):
        backend = RdbBackend(TEST_CH_URL)
        _exec_sql(backend.conn, "drop database if exists t")
        _exec_sql(backend.conn, "create database t")
        _exec_sql(backend.conn, "drop database if exists t1")
        _exec_sql(backend.conn, "create database t1")
        _exec_sql(backend.conn, "create table t.test(id int, val String) engine=MergeTree order by tuple()")
        _exec_sql(backend.conn, "insert into t.test values(1, '1'), (2, '2'), (3, '3')")
        self.run_test_table(backend)

    def test_bq_table(self):
        if not base_test.should_run_integration_test("bq"):
            return
        import os

        backend = RdbBackend(
            TEST_BQ_URL,
            credentials=f"{os.environ.get('HOME', '/tmp')}/.bigquery/credential-test.json",
            sql_expr=bigquery_sql_expr,
        )
        try:
            _exec_sql(backend.conn, "drop schema if exists t cascade")
        except Exception as e:
            # BigQuery will throw an exception when deleting a nonexistent dataset even if using [IF EXISTS]
            import re

            if not re.match(
                r"[\s\S]*Permission bigquery.datasets.delete denied on dataset[\s\S]*(or it may not exist)[\s\S]*",
                str(e.args[0]),
            ):
                raise e

        try:
            _exec_sql(backend.conn, "drop schema if exists t1 cascade")
        except Exception as e:
            import re

            if not re.match(
                r"[\s\S]*Permission bigquery.datasets.delete denied on dataset[\s\S]*(or it may not exist)[\s\S]*",
                str(e.args[0]),
            ):
                raise e

        _exec_sql(backend.conn, "create schema t")
        _exec_sql(backend.conn, "create schema t1")
        _exec_sql(backend.conn, "drop table if exists t.test")
        _exec_sql(backend.conn, "create table if not exists t.test(id int, val string)")
        _exec_sql(backend.conn, "insert into t.test values(1, '1'), (2, '2'), (3, '3')")

        from datetime import timedelta, timezone

        tz = timezone(timedelta(hours=0))
        self.run_test_table(backend, timezone=tz)

    def test_pg_backend(self):
        backend = RdbBackend(TEST_PG_URL)
        _exec_sql(backend.conn, "drop schema if exists t cascade")
        _exec_sql(backend.conn, "create schema t")
        _exec_sql(backend.conn, "create table t.test(id int, val varchar(100))")
        _exec_sql(backend.conn, "insert into t.test values(1, '1'), (2, '2'), (3, '3')")
        _exec_sql(backend.conn, "create table t.dynamic_partition_test(id int, val varchar(100), a varchar(100))")
        _exec_sql(
            backend.conn,
            (
                "insert into t.dynamic_partition_test values(1, '1', '2021-01-01'), "
                "(2, '2', '2021-01-02'),(3, '3', '2021-01-03')"
            ),
        )

        self.run_test_backend(backend)

    def test_ch_backend(self):
        backend = RdbBackend(TEST_CH_URL)
        _exec_sql(backend.conn, "drop database if exists t")
        _exec_sql(backend.conn, "create database t")
        _exec_sql(backend.conn, "create table t.test(id int, val String) engine=MergeTree order by tuple()")
        _exec_sql(backend.conn, "insert into t.test values(1, '1'), (2, '2'), (3, '3')")
        _exec_sql(
            backend.conn,
            "create table t.dynamic_partition_test(id int, val String, a DateTime) engine=MergeTree order by tuple()",
        )
        _exec_sql(
            backend.conn,
            (
                "insert into t.dynamic_partition_test values(1, '1', '2021-01-01 00:00:00'), "
                "(2, '2', '2021-01-02 00:00:00'), (3, '3', '2021-01-03 00:00:00')"
            ),
        )
        self.run_test_backend(backend)

    def test_bq_backend(self):
        if not base_test.should_run_integration_test("bq"):
            return
        import os

        backend = RdbBackend(
            TEST_BQ_URL,
            credentials=f"{os.environ.get('HOME', '/tmp')}/.bigquery/credential-test.json",
            sql_expr=bigquery_sql_expr,
        )
        _exec_sql(backend.conn, "drop schema if exists t cascade")
        _exec_sql(backend.conn, "create schema if not exists t")
        _exec_sql(backend.conn, "create table if not exists t.test(id int, val string)")
        _exec_sql(backend.conn, "insert into t.test values(1, '1'), (2, '2'), (3, '3')")
        _exec_sql(backend.conn, "create table if not exists t.dynamic_partition_test(id int, val string, a date)")
        _exec_sql(
            backend.conn,
            (
                "insert into t.dynamic_partition_test values(1, '1', '2021-01-01'), (2, '2', '2021-01-02'), (3, '3',"
                " '2021-01-03')"
            ),
        )
        self.run_test_backend(backend)

    def run_test_table(self, backend: RdbBackend, timezone=None):
        table = backend.exec_sql("select * from t.test")
        self.assertFalse(table.is_empty())

        table = table.limit(0)
        self.assertTrue(table.is_empty())

        backend.exec_native_sql(backend.sql_dialect.rename_table_sql("t.test", "t.test1"))
        table = backend.exec_sql("select * from t.test1 order by id").with_column("a", backend.sql_expr.for_value(1))
        self.assertFalse(table.is_empty())
        self.assertTrue(table.count(), 3)
        self.assertListEqual(
            table.collect(),
            [
                RdbRow(["id", "val", "a"], (1, "1", 1)),
                RdbRow(["id", "val", "a"], (2, "2", 1)),
                RdbRow(["id", "val", "a"], (3, "3", 1)),
            ],
        )
        self.assertEqual(table.first(), RdbRow(["id", "val", "a"], (1, "1", 1)))
        self.assertListEqual(table.field_names(), ["id", "val", "a"])
        self.assertEqual(table.limit(2).count(), 2)

        backend.exec_native_sql(backend.sql_dialect.create_table_sql("t.test2", "select * from t.test1"))
        table = backend.exec_sql("select * from t.test2 order by id").with_column("a", backend.sql_expr.for_value("1"))
        self.assertEqual(table.first(), RdbRow(["id", "val", "a"], (1, "1", "1")))

        table = backend.exec_sql("select * from t.test2 order by id").with_column("a", backend.sql_expr.for_value(1.1))
        self.assertEqual(table.first(), RdbRow(["id", "val", "a"], (1, "1", 1.1)))

        table = backend.exec_sql("select * from t.test2 order by id").with_column(
            "a", backend.sql_expr.for_value(dt("2020-01-01 11:11:11"))
        )
        self.assertEqual(
            table.first(),
            RdbRow(["id", "val", "a"], (1, "1", base_test.dt_zone("2020-01-01 11:11:11", timezone=timezone))),
        )

        table.show(1)

        backend.exec_native_sql(backend.sql_dialect.create_view_sql("t.test2_view", "select * from t.test2"))
        backend.exec_native_sql(backend.sql_dialect.rename_view_sql("t.test2_view", "t.test2_view_new"))
        table = backend.exec_sql("select * from t.test2_view_new order by id")
        self.assertEqual(table.first(), RdbRow(["id", "val"], (1, "1")))

        backend.exec_native_sql(backend.sql_dialect.rename_table_db_sql("t.test2", "t1"))
        table = backend.exec_sql("select * from t1.test2 order by id")
        self.assertEqual(table.first(), RdbRow(["id", "val"], (1, "1")))

    def run_test_backend(self, backend: RdbBackend):
        backend.create_empty_table()  # should not raise exception

        backend.create_temp_table(backend.exec_sql("select * from t.test order by id limit 2"), "test_limit")
        self.assertListEqual(
            backend.exec_sql(f"select * from {backend.temp_schema}.test_limit order by id").collect(),
            [RdbRow(["id", "val"], (1, "1")), RdbRow(["id", "val"], (2, "2"))],
        )

        self.assertTrue(backend.table_exists(TableMeta("t.test")))
        self.assertFalse(backend.table_exists(TableMeta("t.test_xx")))
        self.assertTrue(backend.table_exists(TableMeta("test_limit")))

        # save without transformation or partitions, should create
        backend.create_cache_table(backend.exec_sql("select * from t.test"), "test")
        backend.save_table(TableMeta("test"), TableMeta("t.xx0"), SaveMode.overwrite, True)
        self.assertListEqual(
            backend.exec_sql("select * from t.xx0 order by id").collect(),
            [RdbRow(["id", "val"], (1, "1")), RdbRow(["id", "val"], (2, "2")), RdbRow(["id", "val"], (3, "3"))],
        )

        self.assertRaisesRegex(
            Exception,
            re.compile(r".* cannot save table.*"),
            lambda: backend.save_table(TableMeta("test_limit"), TableMeta("t.xx"), SaveMode.overwrite, False),
        )

        # first save without partitions, should create
        backend.save_table(TableMeta("test_limit"), TableMeta("t.xx"), SaveMode.overwrite, True)
        self.assertListEqual(
            backend.exec_sql("select * from t.xx order by id").collect(),
            [RdbRow(["id", "val"], (1, "1")), RdbRow(["id", "val"], (2, "2"))],
        )

        # second save without partitions, should overwrite
        backend.exec_native_sql(backend.sql_dialect.drop_view_sql("test_limit"))
        backend.create_cache_table(backend.exec_sql("select * from t.test order by id limit 2"), "test_limit")
        backend.save_table(TableMeta("test_limit"), TableMeta("t.xx"), SaveMode.overwrite, True)
        self.assertListEqual(
            backend.exec_sql("select * from t.xx order by id").collect(),
            [RdbRow(["id", "val"], (1, "1")), RdbRow(["id", "val"], (2, "2"))],
        )

        # third save without partitions, should create extra row
        backend.exec_native_sql(backend.sql_dialect.drop_view_sql("test_limit"))
        backend.create_cache_table(backend.exec_sql("select * from t.test order by id limit 3"), "test_limit")
        backend.save_table(TableMeta("test_limit"), TableMeta("t.xx"), SaveMode.overwrite, True)
        self.assertListEqual(
            backend.exec_sql("select * from t.xx order by id").collect(),
            [RdbRow(["id", "val"], (1, "1")), RdbRow(["id", "val"], (2, "2")), RdbRow(["id", "val"], (3, "3"))],
        )

        if backend.is_bq:
            mock_dt_1, mock_dt_2, mock_dt_3 = date("2021-01-01"), date("2021-01-02"), date("2021-01-03")
        elif backend.is_pg:
            mock_dt_1, mock_dt_2, mock_dt_3 = "2021-01-01", "2021-01-02", "2021-01-03"
        else:
            mock_dt_1, mock_dt_2, mock_dt_3 = (
                dt("2021-01-01 00:00:00"),
                dt("2021-01-02 00:00:00"),
                dt("2021-01-03 00:00:00"),
            )

        backend.exec_native_sql(backend.sql_dialect.drop_view_sql("test_limit"))
        backend.create_cache_table(backend.exec_sql("select id from t.test limit 2"), "test_limit")
        self.assertRaisesRegex(
            Exception,
            re.compile(r"source_cols does not contain target_cols.*"),
            lambda: backend.save_table(
                TableMeta("test_limit"),
                TableMeta("t.xx", partitions=[Partition("a", mock_dt_1)]),
                SaveMode.overwrite,
                True,
            ),
        )

        _exec_sql(backend.conn, "drop table if exists t.xx")

        # first save with partitions, should create
        backend.exec_native_sql(backend.sql_dialect.drop_view_sql("test_limit"))
        backend.create_cache_table(backend.exec_sql("select * from t.test order by id limit 2"), "test_limit")
        backend.save_table(
            TableMeta("test_limit"), TableMeta("t.xx", partitions=[Partition("a", mock_dt_1)]), SaveMode.overwrite, True
        )
        self.assertListEqual(
            backend.exec_sql("select * from t.xx order by id").collect(),
            [RdbRow(["id", "val", "a"], (1, "1", mock_dt_1)), RdbRow(["id", "val", "a"], (2, "2", mock_dt_1))],
        )

        # second save with partitions, should overwrite
        backend.exec_native_sql(backend.sql_dialect.drop_view_sql("test_limit"))
        backend.create_cache_table(backend.exec_sql("select * from t.test order by id limit 2"), "test_limit")
        backend.save_table(
            TableMeta("test_limit"), TableMeta("t.xx", partitions=[Partition("a", mock_dt_2)]), SaveMode.overwrite, True
        )
        self.assertListEqual(
            backend.exec_sql("select * from t.xx order by a, id").collect(),
            [
                RdbRow(["id", "val", "a"], (1, "1", mock_dt_1)),
                RdbRow(["id", "val", "a"], (2, "2", mock_dt_1)),
                RdbRow(["id", "val", "a"], (1, "1", mock_dt_2)),
                RdbRow(["id", "val", "a"], (2, "2", mock_dt_2)),
            ],
        )

        # third save with partitions, should overwrite
        backend.exec_native_sql(backend.sql_dialect.drop_view_sql("test_limit"))
        backend.create_cache_table(backend.exec_sql("select * from t.test order by id limit 2"), "test_limit")
        backend.save_table(
            TableMeta("test_limit"),
            TableMeta("t.xx", partitions=[Partition("a", mock_dt_2)]),
            SaveMode.overwrite,
            False,
        )
        self.assertListEqual(
            backend.exec_sql("select * from t.xx order by a, id").collect(),
            [
                RdbRow(["id", "val", "a"], (1, "1", mock_dt_1)),
                RdbRow(["id", "val", "a"], (2, "2", mock_dt_1)),
                RdbRow(["id", "val", "a"], (1, "1", mock_dt_2)),
                RdbRow(["id", "val", "a"], (2, "2", mock_dt_2)),
            ],
        )

        # fourth save with partitions, should append
        backend.exec_native_sql(backend.sql_dialect.drop_view_sql("test_limit"))
        backend.create_cache_table(backend.exec_sql("select * from t.test order by id limit 2"), "test_limit")
        backend.save_table(
            TableMeta("test_limit"), TableMeta("t.xx", partitions=[Partition("a", mock_dt_2)]), SaveMode.append, True
        )
        self.assertListEqual(
            backend.exec_sql("select * from t.xx order by a, id").collect(),
            [
                RdbRow(["id", "val", "a"], (1, "1", mock_dt_1)),
                RdbRow(["id", "val", "a"], (2, "2", mock_dt_1)),
                RdbRow(["id", "val", "a"], (1, "1", mock_dt_2)),
                RdbRow(["id", "val", "a"], (1, "1", mock_dt_2)),
                RdbRow(["id", "val", "a"], (2, "2", mock_dt_2)),
                RdbRow(["id", "val", "a"], (2, "2", mock_dt_2)),
            ],
        )

        # first save with dynamic partitions, create
        _exec_sql(backend.conn, "drop table if exists t.xx1")
        backend.create_cache_table(
            backend.exec_sql("select * from t.dynamic_partition_test order by id limit 3"),
            "dynamic_partition_test_limit",
        )
        backend.save_table(
            TableMeta("dynamic_partition_test_limit"),
            TableMeta("t.xx1", partitions=[Partition("a", None)]),
            SaveMode.overwrite,
            True,
        )
        self.assertListEqual(
            backend.exec_sql("select * from t.xx1 order by a, id").collect(),
            [
                RdbRow(["id", "val", "a"], (1, "1", mock_dt_1)),
                RdbRow(["id", "val", "a"], (2, "2", mock_dt_2)),
                RdbRow(["id", "val", "a"], (3, "3", mock_dt_3)),
            ],
        )

        # second save with dynamic partitions, overwrite
        backend.exec_native_sql(backend.sql_dialect.drop_view_sql("dynamic_partition_test_limit"))
        backend.create_cache_table(
            backend.exec_sql("select * from t.dynamic_partition_test order by id limit 3"),
            "dynamic_partition_test_limit",
        )
        backend.save_table(
            TableMeta("dynamic_partition_test_limit"),
            TableMeta("t.xx1", partitions=[Partition("a", None)]),
            SaveMode.overwrite,
            False,
        )
        self.assertListEqual(
            backend.exec_sql("select * from t.xx1 order by a, id").collect(),
            [
                RdbRow(["id", "val", "a"], (1, "1", mock_dt_1)),
                RdbRow(["id", "val", "a"], (2, "2", mock_dt_2)),
                RdbRow(["id", "val", "a"], (3, "3", mock_dt_3)),
            ],
        )

        # third save with dynamic partitions, append
        backend.exec_native_sql(backend.sql_dialect.drop_view_sql("dynamic_partition_test_limit"))
        backend.create_cache_table(
            backend.exec_sql("select * from t.dynamic_partition_test order by id limit 3"),
            "dynamic_partition_test_limit",
        )
        backend.save_table(
            TableMeta("dynamic_partition_test_limit"),
            TableMeta("t.xx1", partitions=[Partition("a", None)]),
            SaveMode.append,
            False,
        )
        self.assertListEqual(
            backend.exec_sql("select * from t.xx1 order by a, id").collect(),
            [
                RdbRow(["id", "val", "a"], (1, "1", mock_dt_1)),
                RdbRow(["id", "val", "a"], (1, "1", mock_dt_1)),
                RdbRow(["id", "val", "a"], (2, "2", mock_dt_2)),
                RdbRow(["id", "val", "a"], (2, "2", mock_dt_2)),
                RdbRow(["id", "val", "a"], (3, "3", mock_dt_3)),
                RdbRow(["id", "val", "a"], (3, "3", mock_dt_3)),
            ],
        )

        # fourth save with dynamic partitions, overwrite
        backend.exec_native_sql(backend.sql_dialect.drop_view_sql("dynamic_partition_test_limit"))
        backend.create_cache_table(
            backend.exec_sql("select * from t.dynamic_partition_test order by id limit 3"),
            "dynamic_partition_test_limit",
        )
        backend.save_table(
            TableMeta("dynamic_partition_test_limit"),
            TableMeta("t.xx1", partitions=[Partition("a", None)]),
            SaveMode.overwrite,
            False,
        )
        self.assertListEqual(
            backend.exec_sql("select * from t.xx1 order by a, id").collect(),
            [
                RdbRow(["id", "val", "a"], (1, "1", mock_dt_1)),
                RdbRow(["id", "val", "a"], (2, "2", mock_dt_2)),
                RdbRow(["id", "val", "a"], (3, "3", mock_dt_3)),
            ],
        )

        # first save with non-existing target_table, create
        backend.exec_native_sql(backend.sql_dialect.drop_view_sql("dynamic_partition_test_limit"))
        backend.create_cache_table(
            backend.exec_sql("select * from t.dynamic_partition_test order by id limit 3"),
            "dynamic_partition_test_limit",
        )
        backend.save_table(
            TableMeta("dynamic_partition_test_limit"),
            TableMeta("t_created_db.created_table", partitions=[Partition("a", None)]),
            SaveMode.overwrite,
            True,
        )
        self.assertListEqual(
            backend.exec_sql("select * from t_created_db.created_table order by a, id").collect(),
            [
                RdbRow(["id", "val", "a"], (1, "1", mock_dt_1)),
                RdbRow(["id", "val", "a"], (2, "2", mock_dt_2)),
                RdbRow(["id", "val", "a"], (3, "3", mock_dt_3)),
            ],
        )

        pre_temp_db = backend.temp_schema
        backend.reset()
        cur_temp_db = backend.temp_schema
        self.assertNotEqual(pre_temp_db, cur_temp_db)


if __name__ == "__main__":
    unittest.main()
