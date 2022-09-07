.. Easy SQL documentation master file, created by
   sphinx-quickstart on Wed Apr 27 16:59:16 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Easy SQL's documentation!
====================================

Easy SQL is built to ease the data ETL development process.
With Easy SQL, you can develop your ETL in SQL in an imperative way.

It defines a few simple syntax on top of standard SQL, with which SQL could be executed one by one.
Easy SQL also provides a processor to handle all the new syntax.

Since this is SQL agnostic, any SQL engine could be plugged-in as a backend.
There are built-in supported for several popular SQL engines, including SparkSQL, PostgreSQL, Clickhouse, Aliyun Maxcompute, Google BigQuery.
More will be added in the near future.

Contents
--------

.. toctree::
   :maxdepth: 6

   easy_sql/easy_sql.md
   easy_sql/build_install.md
   easy_sql/quick_start.md
   easy_sql/syntax.md
   easy_sql/debug.md
   easy_sql/testing.md
   easy_sql/linter.md
   easy_sql/functions.md
   easy_sql/udfs.md
   easy_sql/variables.md
   easy_sql/backend/flink.md
   autoapi/index



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
