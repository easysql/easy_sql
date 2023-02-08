download-flink-jars:
	test -f lib/flink/jars/flink-connector-jdbc-1.15.1.jar || wget -P lib/flink/jars https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/1.15.1/flink-connector-jdbc-1.15.1.jar
	test -f lib/flink/jars/flink-sql-connector-hive-3.1.2_2.12-1.15.1.jar || wget -P lib/flink/jars https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.2_2.12/1.15.1/flink-sql-connector-hive-3.1.2_2.12-1.15.1.jar
	test -f lib/flink/jars/postgresql-42.2.14.jar || wget -P lib/flink/jars https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.14/postgresql-42.2.14.jar
	test -f lib/flink/jars/flink-sql-connector-postgres-cdc-2.3.0.jar || wget -P lib/flink/jars https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-postgres-cdc/2.3.0/flink-sql-connector-postgres-cdc-2.3.0.jar
	test -f lib/flink/jars/hudi-flink1.15-bundle-0.12.2.jar || wget -P lib/flink/jars https://repo1.maven.org/maven2/org/apache/hudi/hudi-flink1.15-bundle/0.12.2/hudi-flink1.15-bundle-0.12.2.jar
