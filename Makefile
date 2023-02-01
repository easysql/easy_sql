test-coverage:
	export PYSPARK_PYTHON=python3 && export PYSPARK_DRIVER_PYTHON=python3 && \
 		python3 -m coverage run -m pytest
	python3 -m coverage report -m
	- mkdir build
	- rm -r build/coverage
	python3 -m coverage html

unit-test:
	export PYSPARK_PYTHON=python3 && export PYSPARK_DRIVER_PYTHON=python3 && \
 		python3 -m pytest

# Note: env var named PG_URL and CLICKHOUSE_URL must be set to run e2e test
e2e-test:
	cd test && docker build . --build-arg PG_URL=${PG_URL} --build-arg CLICKHOUSE_URL=${CLICKHOUSE_URL}

echo-var:
	echo ${PG_URL} ${CLICKHOUSE_URL}

e2e-test-spark:
	python3 -m easy_sql.data_process -f test/sample_etl.spark.sql

e2e-test-postgres:
	python3 -m easy_sql.data_process -f test/sample_etl.postgres.sql

e2e-test-clickhouse:
	python3 -m easy_sql.data_process -f test/sample_etl.clickhouse.sql

e2e-test-flink-postgres:
	python3 -m easy_sql.data_process -f test/sample_etl.flink.postgres.sql

e2e-test-flink-streaming:
	python3 -m easy_sql.data_process -f test/sample_etl.flink.postgres-cdc.sql
	python3 -m easy_sql.data_process -f test/sample_etl.flink.postgres-cdc.multi-sink.sql
	python3 -m easy_sql.data_process -f test/sample_etl.flink.postgres-hudi.sql

e2e-test-flink-hive:
	python3 -m easy_sql.data_process -f test/sample_etl.flink.hive.sql

test-coverage-all:
	export PYSPARK_PYTHON=python3 && export PYSPARK_DRIVER_PYTHON=python3 && \
 		PG_URL=${PG_URL} CLICKHOUSE_URL=${CLICKHOUSE_URL} python3 -m coverage run -m pytest -o python_files=*test.py
	python3 -m coverage report -m
	python3 -m coverage xml

package-pip:
	poetry build

upload-test-pip:
	rm -rf ./dist
	poetry publish -r testpypi --build

install-test-pip:
	pip3 uninstall easy_sql-easy_sql
	python3 -m pip install --index-url https://test.pypi.org/simple/ easy_sql-easy_sql[cli]

upload-pip:
	rm -rf ./dist
	poetry publish --build

download-flink-jars:
	test -f test/flink/jars/flink-connector-jdbc-1.15.1.jar || wget -P test/flink/jars https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/1.15.1/flink-connector-jdbc-1.15.1.jar
	test -f test/flink/jars/flink-sql-connector-hive-3.1.2_2.12-1.15.1.jar || wget -P test/flink/jars https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.2_2.12/1.15.1/flink-sql-connector-hive-3.1.2_2.12-1.15.1.jar
	test -f test/flink/jars/postgresql-42.2.14.jar || wget -P test/flink/jars https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.14/postgresql-42.2.14.jar
	test -f test/flink/jars/flink-sql-connector-postgres-cdc-2.3.0.jar || wget -P test/flink/jars https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-postgres-cdc/2.3.0/flink-sql-connector-postgres-cdc-2.3.0.jar
	test -f test/flink/jars/hudi-flink1.15-bundle-0.12.2.jar || wget -P test/flink/jars https://repo1.maven.org/maven2/org/apache/hudi/hudi-flink1.15-bundle/0.12.2/hudi-flink1.15-bundle-0.12.2.jar

install-flink-backend: download-flink-jars
	poetry install -E 'cli pg linter'
	poetry run pip install apache-flink==1.15.1
