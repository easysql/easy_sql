SCALA_BIN=/usr/local/bin
SCALA_CP="/Users/yuewu/.pyenv/versions/3.8.13/lib/python3.8/site-packages/pyflink/lib/*"

jar:
	- rm -r classes
	mkdir -pv classes
	${SCALA_BIN}/scalac -nobootcp -cp ${SCALA_CP} -d classes your/company/*.scala
	cd classes && jar -cvf ../udf.jar .
