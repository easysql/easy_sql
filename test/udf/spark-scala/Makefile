SCALA_BIN=~/dev/sdks/scala-2.12.10/bin
SCALA_CP="/usr/local/lib/python3.8/site-packages/pyspark/jars/*"

jar:
	- rm -r classes
	mkdir -pv classes
	${SCALA_BIN}/scalac -nobootcp -cp ${SCALA_CP} -d classes your/company/*.scala
	cd classes && jar -cvf ../udf.jar .
