import re
import unittest
from functools import partial

from easy_sql import base_test
from easy_sql.base_test import dt
from easy_sql.sql_processor.backend import Partition, SaveMode, TableMeta
from easy_sql.sql_processor.backend.maxcompute import MaxComputeBackend, MaxComputeRow


class MaxComputeTest(unittest.TestCase):
    def test_mc_table(self):
        if not base_test.should_run_integration_test("mc"):
            return

        # TODO: fetch the account info from environment
        odps_parms = {
            "access_id": "xx",
            "secret_access_key": "xx",
            "project": "xx",
            "endpoint": "http://service.cn-hangzhou.maxcompute.aliyun.com/api",
        }
        backend = MaxComputeBackend(**odps_parms)  # type: ignore

        temp_table_name = "test_mc_unit_test"
        backend.conn.execute_sql(f"drop table if exists {temp_table_name}")
        backend.conn.execute_sql(f"create table {temp_table_name}(id int, val varchar(1000))")
        backend.conn.execute_sql(f"insert into {temp_table_name} values(1, '1'), (2, '2'), (3, '3')")

        self.run_test_table(backend, temp_table_name)

        backend.conn.execute_sql(f"drop table if exists {temp_table_name}")
        backend.clear_temp_tables()

    def run_test_table(self, backend: MaxComputeBackend, table_name):
        table = backend.exec_sql(f"select * from {table_name}")
        self.assertFalse(table.is_empty())

        table = table.limit(0)
        self.assertTrue(table.is_empty())

        table = backend.exec_sql(f"select * from {table_name} order by id").with_column("a", 1)
        self.assertFalse(table.is_empty())
        self.assertTrue(table.count(), 3)
        cols, types = ["id", "val", "a"], ["int", "varchar(1000)", "int"]
        self.assertListEqual(
            table.collect(),
            [
                MaxComputeRow.from_schema_meta(cols, types, (1, "1", 1)),
                MaxComputeRow.from_schema_meta(cols, types, (2, "2", 1)),
                MaxComputeRow.from_schema_meta(cols, types, (3, "3", 1)),
            ],
        )
        self.assertEqual(table.first(), MaxComputeRow.from_schema_meta(cols, types, (1, "1", 1)))
        self.assertListEqual(table.field_names(), cols)
        self.assertEqual(table.limit(2).count(), 2)

        table = backend.exec_sql(f"select * from {table_name} order by id").with_column("a", "1")
        cols, types = ["id", "val", "a"], ["int", "varchar(1000)", "string"]
        self.assertEqual(table.first(), MaxComputeRow.from_schema_meta(cols, types, (1, "1", "1")))

        table = backend.exec_sql(f"select * from {table_name} order by id").with_column("a", 1.1)
        cols, types = ["id", "val", "a"], ["int", "varchar(1000)", "float"]
        self.assertEqual(table.first(), MaxComputeRow.from_schema_meta(cols, types, (1, "1", 1.1)))

        table = backend.exec_sql(f"select * from {table_name} order by id").with_column("a", dt("2020-01-01 11:11:11"))
        cols, types = ["id", "val", "a"], ["int", "varchar(1000)", "timestamp"]
        self.assertEqual(
            table.first(), MaxComputeRow.from_schema_meta(cols, types, (1, "1", dt("2020-01-01 11:11:11")))
        )

        table.show(1)

    def test_mc_backend(self):
        if not base_test.should_run_integration_test("mc"):
            return

        # TODO: fetch the account info from environment
        odps_parms = {
            "access_id": "xx",
            "secret_access_key": "xx",
            "project": "xx",
            "endpoint": "http://service.cn-hangzhou.maxcompute.aliyun.com/api",
        }
        backend = MaxComputeBackend(**odps_parms)  # type: ignore

        temp_table_name = "test_mc_unit_test"
        backend.conn.execute_sql(f"drop table if exists {temp_table_name}")
        backend.conn.execute_sql(f"create table {temp_table_name}(id int, val varchar(1000))")
        backend.conn.execute_sql(f"insert into {temp_table_name} values(1, '1'), (2, '2'), (3, '3')")

        self.run_test_backend(backend, temp_table_name)

        backend.conn.execute_sql(f"drop table if exists {temp_table_name}")
        backend.clear_temp_tables()

    def run_test_backend(self, backend: MaxComputeBackend, table_name):
        backend.create_empty_table()  # should not raise exception

        table = backend.exec_sql(f"select * from {table_name} limit 2")
        backend.create_temp_table(table, "test_limit")
        sc = table.schema
        self.assertListEqual(
            backend.exec_sql("select * from test_limit order by id").collect(),
            [MaxComputeRow(schema=sc, values=(1, "1")), MaxComputeRow(schema=sc, values=(2, "2"))],
        )
        self.assertTrue(backend.table_exists(TableMeta(table_name)))
        self.assertFalse(backend.table_exists(TableMeta(f"{table_name}_xxx")))
        self.assertTrue(backend.table_exists(TableMeta("test_limit")))

        # save without transformation or partitions, should create
        backend.exec_native_sql("drop table if exists xx0")
        backend.create_cache_table(backend.exec_sql(f"select * from {table_name}"), "cache_test")
        backend.save_table(TableMeta("cache_test"), TableMeta("xx0"), SaveMode.overwrite, True)
        self.assertListEqual(
            backend.exec_sql("select * from xx0 order by id").collect(),
            [
                MaxComputeRow(schema=sc, values=(1, "1")),
                MaxComputeRow(schema=sc, values=(2, "2")),
                MaxComputeRow(schema=sc, values=(3, "3")),
            ],
        )
        backend.exec_native_sql("drop table if exists xx0")

        backend.exec_native_sql("drop table if exists xx")
        self.assertRaisesRegex(
            Exception,
            re.compile(r".* cannot save table.*"),
            lambda: backend.save_table(TableMeta("test_limit"), TableMeta("xx"), SaveMode.overwrite, False),
        )
        backend.exec_native_sql("drop table if exists xx")

        # first save without partitions, should create
        backend.save_table(TableMeta("test_limit"), TableMeta("xx"), SaveMode.overwrite, True)
        self.assertListEqual(
            backend.exec_sql("select * from xx order by id").collect(),
            [MaxComputeRow(schema=sc, values=(1, "1")), MaxComputeRow(schema=sc, values=(2, "2"))],
        )

        # second save without partitions, should overwrite
        backend.exec_native_sql("drop view if exists test_limit")
        backend.create_cache_table(backend.exec_sql(f"select * from {table_name} limit 2"), "test_limit")
        backend.save_table(TableMeta("test_limit"), TableMeta("xx"), SaveMode.overwrite, True)
        self.assertListEqual(
            backend.exec_sql("select * from xx order by id").collect(),
            [MaxComputeRow(schema=sc, values=(1, "1")), MaxComputeRow(schema=sc, values=(2, "2"))],
        )

        backend.exec_native_sql("drop view if exists test_limit")
        backend.create_cache_table(backend.exec_sql(f"select * from {table_name} limit 2"), "test_limit")
        with self.assertRaises(Exception):  # noqa: B017
            backend.save_table(
                TableMeta("test_limit"),
                TableMeta("xx", partitions=[Partition("a", dt("2021-01-01 00:00:00"))]),
                SaveMode.overwrite,
                True,
            )
        backend.exec_native_sql("drop table if exists xx")

        # first save with partitions, should create
        backend.exec_native_sql("drop view if exists test_limit")
        backend.create_cache_table(backend.exec_sql(f"select * from {table_name} limit 2"), "test_limit")
        backend.save_table(
            TableMeta("test_limit"),
            TableMeta("xx", partitions=[Partition("a", dt("2021-01-01 00:00:00"))]),
            SaveMode.overwrite,
            True,
        )
        mc_row = partial(
            MaxComputeRow.from_schema_meta,
            cols=["id", "val"],
            types=["int", "string"],
            pt_cols=["a"],
            pt_types=["string"],
        )
        self.assertListEqual(
            backend.exec_sql("select * from xx order by id").collect(),
            [
                mc_row(values=(1, "1", dt("2021-01-01 00:00:00"))),
                mc_row(values=(2, "2", dt("2021-01-01 00:00:00"))),
            ],
        )

        # second save with partitions, should overwrite
        backend.exec_native_sql("drop view if exists test_limit")
        backend.create_cache_table(backend.exec_sql(f"select * from {table_name} limit 2"), "test_limit")
        backend.save_table(
            TableMeta("test_limit"),
            TableMeta("xx", partitions=[Partition("a", dt("2021-01-02 00:00:00"))]),
            SaveMode.overwrite,
            True,
        )
        self.assertListEqual(
            backend.exec_sql("select * from xx order by a, id").collect(),
            [
                mc_row(values=(1, "1", dt("2021-01-01 00:00:00"))),
                mc_row(values=(2, "2", dt("2021-01-01 00:00:00"))),
                mc_row(values=(1, "1", dt("2021-01-02 00:00:00"))),
                mc_row(values=(2, "2", dt("2021-01-02 00:00:00"))),
            ],
        )

        # third save with partitions, should overwrite
        backend.exec_native_sql("drop view if exists test_limit")
        backend.create_cache_table(backend.exec_sql(f"select * from {table_name} limit 2"), "test_limit")
        backend.save_table(
            TableMeta("test_limit"),
            TableMeta("xx", partitions=[Partition("a", dt("2021-01-02 00:00:00"))]),
            SaveMode.overwrite,
            False,
        )
        self.assertListEqual(
            backend.exec_sql("select * from xx order by a, id").collect(),
            [
                mc_row(values=(1, "1", dt("2021-01-01 00:00:00"))),
                mc_row(values=(2, "2", dt("2021-01-01 00:00:00"))),
                mc_row(values=(1, "1", dt("2021-01-02 00:00:00"))),
                mc_row(values=(2, "2", dt("2021-01-02 00:00:00"))),
            ],
        )

        # fourth save with partitions, should append
        backend.exec_native_sql("drop view if exists test_limit")
        backend.create_cache_table(backend.exec_sql(f"select * from {table_name} limit 2"), "test_limit")
        backend.save_table(
            TableMeta("test_limit"),
            TableMeta("xx", partitions=[Partition("a", dt("2021-01-02 00:00:00"))]),
            SaveMode.append,
            False,
        )
        self.assertListEqual(
            backend.exec_sql("select * from xx order by a, id").collect(),
            [
                mc_row(values=(1, "1", dt("2021-01-01 00:00:00"))),
                mc_row(values=(2, "2", dt("2021-01-01 00:00:00"))),
                mc_row(values=(1, "1", dt("2021-01-02 00:00:00"))),
                mc_row(values=(1, "1", dt("2021-01-02 00:00:00"))),
                mc_row(values=(2, "2", dt("2021-01-02 00:00:00"))),
                mc_row(values=(2, "2", dt("2021-01-02 00:00:00"))),
            ],
        )

        backend.exec_native_sql("drop table if exists xx")
