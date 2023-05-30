import unittest
from pathlib import Path

import yaml

from easy_sql.sql_processor.backend.flink import FlinkConfig, FlinkTablesConfig

tc = FlinkTablesConfig(
    {
        "databases": [
            {
                "name": "a",
                "tables": [
                    {"name": "a", "schema": ["a int", "b\tvarchar", "`c` char", "'d' int"]},
                    {"name": "b"},
                ],
            }
        ]
    }
)


class FlinkTablesConfigTest(unittest.TestCase):
    def test_table_fields(self):
        self.assertEquals(tc.table_fields("a.a", ["b"]), ["a", "c", "d"])
        self.assertRaises(Exception, lambda: tc.table_fields("b"))
        self.assertRaises(Exception, lambda: tc.table_fields("a.c"))
        self.assertRaises(Exception, lambda: tc.table_fields("a.b"))

    def test_table_options(self):
        self.assertEquals(tc.table_options({"options": {"a": 1}}, {"name": "a"}), {"a": 1})
        self.assertEquals(
            tc.table_options({"options": {"a": 1}}, {"name": "a", "connector": {"options": {"b": 1}}}), {"a": 1, "b": 1}
        )
        self.assertEquals(
            tc.table_options({"options": {"path": "/a/"}}, {"name": "b"}),
            {"path": "/a/b"},
        )
        self.assertEquals(
            tc.table_options({"options": {"path": "/a"}}, {"name": "b"}),
            {"path": "/a/b"},
        )
        self.assertEquals(
            tc.table_options({"options": {"path": "/a/"}}, {"name": "a", "connector": {"options": {"path": "c"}}}),
            {"path": "c"},
        )


def test_parse_flink_config_from_yml():
    yml = """
connectors:
  pg_cdc:
    options: |
      'connector' = 'postgres-cdc',
      'hostname' = 'postgres',
      'port' = '5432',
      'username' = 'postgres',
      'password' = 'postgres',
      'database-name' = 'postgres',
      'schema-name' = 'public',
      'decoding.plugin.name' = 'pgoutput'
catalogs:
  paimon:
    options: |
      'type' = 'paimon',
      'warehouse' = 'file: ///opt/flink/paimon'
    databases:
      ods:
        tables:
          orders:
            options: |
              'changelog-producer' = 'input'
            partition_by: "dd, hh"
            schema: |
              order_id STRING,
              product_id STRING,
              customer_id STRING,
              purchase_timestamp TIMESTAMP_LTZ,
              dd STRING,
              hh INT,
              pts as PROCTIME(),
              ts as cast(purchase_timestamp as TIMESTAMP_LTZ(3)),
              WATERMARK FOR ts AS ts - INTERVAL '5' SECOND,
              PRIMARY KEY (order_id, dd, hh) NOT ENFORCED
    temporary_tables:
      cdc_orders:
        connector: pg_cdc
        options: |
          'table-name' = 'orders',
          'slot.name' = 'orders'
        schema: |
          order_id STRING,
          product_id STRING,
          customer_id STRING,
          purchase_timestamp TIMESTAMP_LTZ,
          PRIMARY KEY (order_id) NOT ENFORCED
    """
    res: dict = yaml.safe_load(yml)
    config = FlinkConfig.from_dict(res)
    assert "postgres" in config.connectors["pg_cdc"].options

    paimon = config.catalogs["paimon"]
    assert "warehouse" in (paimon.options or "")

    orders = paimon.databases["ods"].tables["orders"]
    assert "input" in (orders.options or "")
    assert orders.partition_by == "dd, hh"
    assert "WATERMARK" in orders.schema

    cdc_orders = paimon.temporary_tables["cdc_orders"]
    assert "purchase_timestamp" in cdc_orders.schema
    assert "slot.name" in (cdc_orders.options or "")
    assert cdc_orders.connector == "pg_cdc"
