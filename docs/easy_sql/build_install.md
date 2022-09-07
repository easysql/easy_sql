# Build and install Easy SQL

Easy SQL is a very light-weight python library. The common Python library conventions are followed.
It's easy to build or install Easy SQL.

## Install Easy SQL

Install Easy SQL using pip: `python3 -m pip install easy_sql-easy_sql[extra,extra]`

Currently we are providing below extras, choose according to your need:
- cli
- linter
- spark
- pg
- clickhouse

## Building Easy SQL

Internally we use `poetry` to manage the dependencies. So make sure you have [installed it](https://python-poetry.org/docs/master/#installation). Package could be built with the following make command: `make package-pip` or just `poetry build`.

After the above command, there will be a file named `easy_sql*.whl` generated in the `dist` folder.
You can install it with command `python3 -m pip install dist/easy_sql*.whl[extra]` or just `poetry install -E 'extra extra'`.

We also provided flink as backend, but because of dependency conflicts between pyspark and apache-flink, you need to install the flink backend manually with the following make command `poetry run make install-flink-backend`.
