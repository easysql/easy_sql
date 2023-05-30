import pytest
import yaml

from easy_sql.sql_processor.backend.flink import FlinkTablesConfig


@pytest.fixture
def config() -> FlinkTablesConfig:
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
      'warehouse' = 'file:///opt/flink/paimon'
    databases:
      ods:
        tables:
          orders:
            options: |
              'changelog-producer'   =   'input'
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
    config = FlinkTablesConfig.from_dict(res)
    return config


def test_parse_flink_config_from_yml(config: FlinkTablesConfig):
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


def test_generate_catalog_ddl(config: FlinkTablesConfig):
    ddl = list(config.generate_catalog_ddl())
    assert len(ddl) == 1
    assert ddl[0][0] == "paimon"
    assert ddl[0][1] == "CREATE CATALOG paimon with ('type' = 'paimon',\n'warehouse' = 'file:///opt/flink/paimon'\n)"


def test_generate_db_ddl(config: FlinkTablesConfig):
    ddl = config.generate_db_ddl()
    assert list(ddl) == ["CREATE database if not exists paimon.ods"]


def test_generate_table_ddl(config: FlinkTablesConfig):
    ddl = list(config.generate_table_ddl())

    assert len(ddl) == 2

    assert (
        ddl[0]
        == "create  table if not exists paimon.ods.orders (order_id STRING,\nproduct_id STRING,\ncustomer_id"
        " STRING,\npurchase_timestamp TIMESTAMP_LTZ,\ndd STRING,\nhh INT,\npts as PROCTIME(),\nts as"
        " cast(purchase_timestamp as TIMESTAMP_LTZ(3)),\nWATERMARK FOR ts AS ts - INTERVAL '5' SECOND,\nPRIMARY KEY"
        " (order_id, dd, hh) NOT ENFORCED\n) partitioned by (dd, hh) with ('changelog-producer' = 'input')"
    )

    assert (
        ddl[1]
        == "create temporary table if not exists cdc_orders (order_id STRING,\nproduct_id STRING,\ncustomer_id"
        " STRING,\npurchase_timestamp TIMESTAMP_LTZ,\nPRIMARY KEY (order_id) NOT ENFORCED\n)  with ('connector' ="
        " 'postgres-cdc' , 'hostname' = 'postgres' , 'port' = '5432' , 'username' = 'postgres' , 'password' ="
        " 'postgres' , 'database-name' = 'postgres' , 'schema-name' = 'public' , 'decoding.plugin.name' = 'pgoutput'"
        " , 'table-name' = 'orders' , 'slot.name' = 'orders')"
    )


def test_from_none_yml():
    cf = FlinkTablesConfig.from_yml(None)
    assert cf is not None
    assert cf.catalogs == {}
    assert cf.connectors == {}
