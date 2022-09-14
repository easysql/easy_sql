from __future__ import annotations

import os
import re
import unittest
from typing import TYPE_CHECKING

from pyflink.common import Row
from pyflink.table import DataTypes
from pyflink.table.schema import Schema
from pyflink.table.table_descriptor import TableDescriptor

from easy_sql import base_test
from easy_sql.base_test import (
    TEST_PG_JDBC_PASSWD,
    TEST_PG_JDBC_URL,
    TEST_PG_JDBC_USER,
    TEST_PG_URL,
)
from easy_sql.sql_processor.backend import FlinkBackend, Partition, SaveMode, TableMeta
from easy_sql.sql_processor.backend.flink import FlinkRow
from easy_sql.sql_processor.backend.rdb import _exec_sql

from .sql_dialect import SqlExpr
from .sql_dialect.postgres import PgSqlDialect

if TYPE_CHECKING:
    from sqlalchemy.engine.base import Connection, Engine


class FlinkTest(unittest.TestCase):
    def test_flink_table(self):
        backend = FlinkBackend()

        table = backend.flink.from_elements([(1, "1"), (2, "2"), (3, "3")], schema=["id", "val"])
        backend.flink.register_table("test", table)

        self.run_test_table(backend)

    def test_flink_backend_pg(self):
        backend = FlinkBackend()

        # just for test
        test_jar_path = "test/flink/jars"
        base_path = os.path.abspath(os.curdir)
        backend.add_jars([os.path.join(base_path, test_jar_path, jar) for jar in os.listdir(test_jar_path)])

        schema = DataTypes.ROW([DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("val", DataTypes.STRING())])
        table = backend.flink.from_elements([(1, "1"), (2, "2"), (3, "3")], schema=schema)
        backend.flink.register_table("test", table)

        self.sql_dialect = PgSqlDialect(SqlExpr())
        from sqlalchemy import create_engine

        self.engine: Engine = create_engine(TEST_PG_URL, isolation_level="AUTOCOMMIT", pool_size=1)
        self.conn: Connection = self.engine.connect()
        # create target table to db
        _exec_sql(self.conn, self.sql_dialect.drop_table_sql("out_put_table"))
        _exec_sql(
            self.conn,
            """
            CREATE TABLE out_put_table (
                id int4 PRIMARY KEY,
                val text
            )
        """,
        )
        backend.exec_native_sql(
            f"""
            CREATE TABLE out_put_table (
                id INT,
                val VARCHAR,
                PRIMARY KEY (id) NOT ENFORCED
            )
            WITH (
                'connector' = 'jdbc',
                'url' = '{TEST_PG_JDBC_URL}',
                'username' = '{TEST_PG_JDBC_USER}',
                'password' = '{TEST_PG_JDBC_PASSWD}',
                'table-name' = 'out_put_table');
        """
        )

        from pyflink.java_gateway import get_gateway
        from pyflink.table.catalog import CatalogBaseTable

        gateway = get_gateway()
        catalog = backend.flink.get_current_catalog()
        database = backend.flink.get_current_database()
        catalog_table = CatalogBaseTable(
            backend.flink._j_tenv.getCatalogManager()
            .getTable(gateway.jvm.ObjectIdentifier.of(catalog, database, "out_put_table"))  # type: ignore
            .get()
            .getTable()
        )
        self.assertEqual("jdbc", catalog_table.get_options().get("connector"))
        self.assertEqual("postgres", catalog_table.get_options().get("username"))
        schema = (
            Schema.new_builder()
            .primary_key("id")
            .column("id", DataTypes.INT())
            .column("val", DataTypes.STRING())
            .build()
        )

        self.run_test_backend_pg(backend)

    def test_flink_backend_hive(self):
        if not base_test.should_run_integration_test("flink_hive"):
            return
        backend = FlinkBackend()

        catalog_name = "hive"
        hive_conf_dir = "/ops/apache-hive/conf"
        backend.exec_native_sql(
            f"""
            CREATE CATALOG testHiveCatalog WITH (
                'type' = '{catalog_name}',
                'hive-conf-dir' = '{hive_conf_dir}'
            );
        """
        )

        schema = DataTypes.ROW([DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("val", DataTypes.STRING())])
        table = backend.flink.from_elements([(1, "1"), (2, "2"), (3, "3")], schema=schema)
        backend.flink.register_table("test", table)

        self.run_test_backend_hive(backend)

    def run_test_table(self, backend: FlinkBackend):
        table = backend.exec_sql("select * from test")
        self.assertFalse(table.is_empty())
        self.assertTrue(table.count(), 3)
        self.assertListEqual(table.field_names(), ["id", "val"])

        first_row = table.first()
        self.assertEqual(first_row, FlinkRow(Row(1, "1"), ["id", "val"]))
        row_dict = first_row.as_dict()
        self.assertEqual(row_dict, {"id": 1, "val": "1"})
        self.assertListEqual(
            table.collect(),
            [
                FlinkRow(Row(1, "1"), ["id", "val"]),
                FlinkRow(Row(2, "2"), ["id", "val"]),
                FlinkRow(Row(3, "3"), ["id", "val"]),
            ],
        )

        row_tuple = first_row.as_tuple()
        self.assertEqual(row_tuple[0], 1)
        self.assertEqual(row_tuple[1], "1")

        from pyflink.table.expressions import col, lit

        table = table.with_column("a", col("val"))
        table = table.with_column("b", lit("haha"))
        self.assertListEqual(
            table.collect(),
            [
                FlinkRow(Row(1, "1", "1", "haha"), ["id", "val", "a", "b"]),
                FlinkRow(Row(2, "2", "2", "haha"), ["id", "val", "a", "b"]),
                FlinkRow(Row(3, "3", "3", "haha"), ["id", "val", "a", "b"]),
            ],
        )
        self.assertEqual(table.first().as_dict(), {"id": 1, "val": "1", "a": "1", "b": "haha"})
        self.assertListEqual(table.field_names(), ["id", "val", "a", "b"])

        table.show(1)

    def run_test_backend_pg(self, backend: FlinkBackend):
        backend.create_empty_table()  # should not raise exception

        backend.flink.create_table(
            "test_table_exist",
            TableDescriptor.for_connector("test-connector")
            .schema(Schema.new_builder().column("f0", DataTypes.STRING()).build())
            .build(),
        )
        self.assertTrue(backend.table_exists(TableMeta("test_table_exist")))
        self.assertFalse(backend.table_exists(TableMeta("t.test_xx")))

        backend.create_temp_table(backend.exec_sql("select * from test order by id limit 2 "), "test_limit")
        self.assertListEqual(
            backend.exec_sql("select * from test_limit").collect(),
            [FlinkRow(Row(1, "1"), ["id", "val"]), FlinkRow(Row(2, "2"), ["id", "val"])],
        )

        backend.create_cache_table(backend.exec_sql("select * from test"), "test_view")
        self.assertListEqual(
            backend.exec_sql("select * from test_view").collect(),
            [
                FlinkRow(Row(1, "1"), ["id", "val"]),
                FlinkRow(Row(2, "2"), ["id", "val"]),
                FlinkRow(Row(3, "3"), ["id", "val"]),
            ],
        )

        self.assertRaisesRegex(
            Exception,
            re.compile(r".* cannot save table.*"),
            lambda: backend.save_table(TableMeta("test_limit"), TableMeta("not_exists"), SaveMode.overwrite),
        )

        exceptionMsg = (
            "org.apache.flink.table.api.ValidationException: INSERT OVERWRITE requires that the underlying"
            " DynamicTableSink of table 'default_catalog.default_database.out_put_table' implements the"
            " SupportsOverwrite interface."
        )
        self.assertRaisesRegex(
            Exception,
            re.compile(exceptionMsg),
            lambda: backend.save_table(TableMeta("test_limit"), TableMeta("out_put_table"), SaveMode.overwrite),
        )

        # first save without transformation or partitions
        backend.save_table(TableMeta("test_view"), TableMeta("out_put_table"), SaveMode.append)
        self.assertListEqual(
            backend.exec_sql("select * from out_put_table order by id").collect(),
            [
                FlinkRow(Row(1, "1"), ["id", "val"]),
                FlinkRow(Row(2, "2"), ["id", "val"]),
                FlinkRow(Row(3, "3"), ["id", "val"]),
            ],
        )

        schema = DataTypes.ROW([DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("val", DataTypes.STRING())])
        append_table = backend.flink.from_elements(
            [(3, "3 has already been updated"), (5, "5"), (6, "6")], schema=schema
        )
        backend.flink.register_table("append_table", append_table)

        backend.save_table(TableMeta("append_table"), TableMeta("out_put_table"), SaveMode.append)
        self.assertListEqual(
            backend.exec_sql("select * from out_put_table order by id").collect(),
            [
                FlinkRow(Row(1, "1"), ["id", "val"]),
                FlinkRow(Row(2, "2"), ["id", "val"]),
                FlinkRow(Row(3, "3 has already been updated"), ["id", "val"]),
                FlinkRow(Row(5, "5"), ["id", "val"]),
                FlinkRow(Row(6, "6"), ["id", "val"]),
            ],
        )

        backend.clean()
        self.assertListEqual(backend.flink.list_temporary_views(), [])

        backend.refresh_table_partitions(TableMeta("out_put_table"))

    def run_test_backend_hive(self, backend: FlinkBackend):
        # first save without transformation or partitions
        backend.save_table(TableMeta("test"), TableMeta("testHiveCatalog.myhive.hive_table"), SaveMode.overwrite)
        self.assertListEqual(
            backend.exec_sql("select * from testHiveCatalog.myhive.hive_table").collect(),
            [
                FlinkRow(Row(1, "1"), ["id", "val"]),
                FlinkRow(Row(2, "2"), ["id", "val"]),
                FlinkRow(Row(3, "3"), ["id", "val"]),
            ],
        )

        backend.save_table(
            TableMeta("testHiveCatalog.myhive.hive_table"),
            TableMeta("testHiveCatalog.myhive.hive_out_table"),
            SaveMode.overwrite,
        )
        self.assertListEqual(
            backend.exec_sql("select * from testHiveCatalog.myhive.hive_out_table").collect(),
            [
                FlinkRow(Row(1, "1"), ["id", "val"]),
                FlinkRow(Row(2, "2"), ["id", "val"]),
                FlinkRow(Row(3, "3"), ["id", "val"]),
            ],
        )

        backend.create_temp_table(backend.exec_sql("select * from test limit 2"), "test_limit")
        self.assertListEqual(
            backend.exec_sql("select * from test_limit").collect(),
            [FlinkRow(Row(1, "1"), ["id", "val"]), FlinkRow(Row(2, "2"), ["id", "val"])],
        )

        backend.save_table(
            TableMeta("test_limit"), TableMeta("testHiveCatalog.myhive.hive_out_table"), SaveMode.overwrite
        )
        self.assertListEqual(
            backend.exec_sql("select * from testHiveCatalog.myhive.hive_out_table").collect(),
            [FlinkRow(Row(1, "1"), ["id", "val"]), FlinkRow(Row(2, "2"), ["id", "val"])],
        )

        schema = DataTypes.ROW([DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("val", DataTypes.STRING())])
        append_table = backend.flink.from_elements(
            [(2, "2 will not be updated in hive, but insert"), (5, "5"), (6, "6")], schema=schema
        )
        backend.flink.register_table("append_table", append_table)

        backend.save_table(
            TableMeta("append_table"), TableMeta("testHiveCatalog.myhive.hive_out_table"), SaveMode.append
        )
        self.assertListEqual(
            backend.exec_sql("select * from testHiveCatalog.myhive.hive_out_table").collect(),
            [
                FlinkRow(Row(1, "1"), ["id", "val"]),
                FlinkRow(Row(2, "2"), ["id", "val"]),
                FlinkRow(Row(2, "2 will not be updated in hive, but insert"), ["id", "val"]),
                FlinkRow(Row(5, "5"), ["id", "val"]),
                FlinkRow(Row(6, "6"), ["id", "val"]),
            ],
        )

        schema = DataTypes.ROW([DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("val", DataTypes.STRING())])
        table = backend.flink.from_elements([], schema=schema)
        backend.flink.register_table("empty_table", table)

        backend.save_table(
            TableMeta("empty_table"), TableMeta("testHiveCatalog.myhive.hive_out_table"), SaveMode.overwrite, True
        )
        self.assertListEqual(backend.exec_sql("select * from testHiveCatalog.myhive.hive_out_table").collect(), [])

        mock_dt_1, mock_dt_2 = "2021-01-01", "2021-01-02"
        # first save with partitions, should create
        backend.save_table(
            TableMeta("test"),
            TableMeta("testHiveCatalog.myhive.hive_out_table_pt", partitions=[Partition("dt", mock_dt_1)]),
            SaveMode.overwrite,
            True,
        )
        self.assertListEqual(
            backend.exec_sql(
                f"select * from testHiveCatalog.myhive.hive_out_table_pt where dt = '{mock_dt_1}' order by id"
            ).collect(),
            [
                FlinkRow(Row(1, "1", mock_dt_1), ["id", "val", "dt"]),
                FlinkRow(Row(2, "2", mock_dt_1), ["id", "val", "dt"]),
                FlinkRow(Row(3, "3", mock_dt_1), ["id", "val", "dt"]),
            ],
        )

        # second save with partitions, should overwrite
        backend.save_table(
            TableMeta("test_limit"),
            TableMeta("testHiveCatalog.myhive.hive_out_table_pt", partitions=[Partition("dt", mock_dt_2)]),
            SaveMode.overwrite,
            True,
        )
        self.assertListEqual(
            backend.exec_sql("select * from testHiveCatalog.myhive.hive_out_table_pt order by id, dt").collect(),
            [
                FlinkRow(Row(1, "1", mock_dt_1), ["id", "val", "dt"]),
                FlinkRow(Row(1, "1", mock_dt_2), ["id", "val", "dt"]),
                FlinkRow(Row(2, "2", mock_dt_1), ["id", "val", "dt"]),
                FlinkRow(Row(2, "2", mock_dt_2), ["id", "val", "dt"]),
                FlinkRow(Row(3, "3", mock_dt_1), ["id", "val", "dt"]),
            ],
        )

        # third save with partitions, should overwrite
        backend.save_table(
            TableMeta("test_limit"),
            TableMeta("testHiveCatalog.myhive.hive_out_table_pt", partitions=[Partition("dt", mock_dt_2)]),
            SaveMode.overwrite,
            True,
        )
        self.assertListEqual(
            backend.exec_sql("select * from testHiveCatalog.myhive.hive_out_table_pt order by id, dt").collect(),
            [
                FlinkRow(Row(1, "1", mock_dt_1), ["id", "val", "dt"]),
                FlinkRow(Row(1, "1", mock_dt_2), ["id", "val", "dt"]),
                FlinkRow(Row(2, "2", mock_dt_1), ["id", "val", "dt"]),
                FlinkRow(Row(2, "2", mock_dt_2), ["id", "val", "dt"]),
                FlinkRow(Row(3, "3", mock_dt_1), ["id", "val", "dt"]),
            ],
        )

        backend.save_table(
            TableMeta("append_table"),
            TableMeta("testHiveCatalog.myhive.hive_out_table_pt", partitions=[Partition("dt", mock_dt_2)]),
            SaveMode.append,
            True,
        )
        self.assertListEqual(
            backend.exec_sql("select * from testHiveCatalog.myhive.hive_out_table_pt order by id, dt, val").collect(),
            [
                FlinkRow(Row(1, "1", mock_dt_1), ["id", "val", "dt"]),
                FlinkRow(Row(1, "1", mock_dt_2), ["id", "val", "dt"]),
                FlinkRow(Row(2, "2", mock_dt_1), ["id", "val", "dt"]),
                FlinkRow(Row(2, "2", mock_dt_2), ["id", "val", "dt"]),
                FlinkRow(Row(2, "2 will not be updated in hive, but insert", mock_dt_2), ["id", "val", "dt"]),
                FlinkRow(Row(3, "3", mock_dt_1), ["id", "val", "dt"]),
                FlinkRow(Row(5, "5", mock_dt_2), ["id", "val", "dt"]),
                FlinkRow(Row(6, "6", mock_dt_2), ["id", "val", "dt"]),
            ],
        )


if __name__ == "__main__":
    unittest.main()
