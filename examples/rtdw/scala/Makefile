SCALA_BIN=~/dev/sdks/scala-2.12.10/bin
FLINK_JAR_PATH=/usr/local/lib/python3.8/site-packages/pyflink/lib/*
SCALA_CP="${FLINK_JAR_PATH}:../lib/flink/jars/*"


ods-jar:
	- rm -r classes
	mkdir -pv classes
	${SCALA_BIN}/scalac -nobootcp -cp ${SCALA_CP} -d classes src/com/easysql/example/*.scala
	cd classes && jar -cvfe ../easysql-example-ods.jar com.easysql.example.PostgresCDC com
