<!-- Notes: this doc need to be updated manually with script: update_doc.py -->

# UDFs

User-defined function is a widely used concept in SQL world. It is a user-programmable routine to extend SQL.

There are two types of user defined functions in SQL:

- `UDF`: User-defined function that acts on one row and return a single value.
- `UDAF`: User-defined aggregate function that acts on multiple rows at once and return a single aggregated value as a result.

Easy SQL provides support to define UDF/UDAF, register them, and then use them in ETL.

Since there are multiple SQL engine backends in Easy SQl, we need to implement UDF/UDAF according to the specific backend being selected.

This document will guide you through to create and use UDF/UDAFs.

## For spark backend

We can create spark UDF/UDAF using scala or python.

It's easy to create a spark UDF. An example is as below:

```scala
val random = udf(() => Math.random())
spark.udf.register("random", random.asNondeterministic())
```

For details, please refer to spark [UDF](https://spark.apache.org/docs/latest/sql-ref-functions-udf-scalar.html) / [UDAF](https://spark.apache.org/docs/latest/sql-ref-functions-udf-aggregate.html) introduction page.

```python
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
import random
random_udf = udf(lambda: int(random.random() * 100), IntegerType()).asNondeterministic()
```

For details, please refer to pyspark [UDF](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.udf.html) introduction page.
There is no support to create UDAF from pyspark yet. We can use collect_list with pyspark UDF to simulate UDAF.

Easy SQL provides support for registering UDF / UDAF in both scala and python.

### Register and use scala UDF/UDAF

To define and register a scala UDF/UDAF, we need to create a scala file with a funciton named `initUdf`.
Below is an example:

```scala
// udfs.scala
package your.company

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

object udfs {
    def initUdfs(spark: SparkSession) {
        val string_set = udf((s: Seq[String]) => s.filter(_ != null).toSet.toArray)
        spark.udf.register("string_set", string_set)
    }
}
```

The next thing is to compile this file and package it into a jar file. An example command line could be:

```bash
# SCALA_BIN is a path to the bin folder of scala sdk directory.
# SCALA_CP is the java class path for compiling. Usually are some jars.
${SCALA_BIN}/scalac -nobootcp -cp ${SCALA_CP} -d classes your/company/*.scala
cd classes && jar -cvf ../udf.jar .
```

After the command above, you'll get a jar file named `udf.jar`.

We can register the above UDFs in ETL configuration block. Below is an example:

```sql
-- backend: spark
-- config: spark.jars=udf.jar
-- config: easy_sql.scala_udf_initializer=your.company.udfs

-- target=log.test_udf
select string_set(array("a", "a", "b")) as stringset
```

Save the file above to a file named `etl_with_udf.sql`, and then run command below to execute the ETL.

```bash
bash -c "$(python3 -m easy_sql.data_process -f etl_with_udf.sql -p)"
```

You'll get a result like:

```
===================== REPORT FOR step-1 ==================
config: StepConfig(target=log.test_udf, condition=None, line_no=5)
sql: select string_set(array("a", "a", "b")) as stringset
status: SUCCEEDED
start time: xxxx-xx-xx xx:xx:xx, end time: xxxx-xx-xx xx:xx:xx, execution time: 2.194115s - 65.29%
messages:
stringset=['a', 'b']
```

### Register and use python UDF

To register a python UDF is much easier. But if you choose to implement UDF in python, there might be a performance issue
since spark application runs in Java and must talk to a python process when calling UDF.

First, we need to define a UDF in a python file:

```python
from typing import List

__all__ = ['string_set']

def string_set(string_arr: List[str]) -> List[str]:
    return list(set(string_arr))
```

Let's assume the file name is `udf.py`.

We can register the above UDFs in ETL configuration block. Below is an example:

```sql
-- backend: spark
-- config: easy_sql.udf_file_path=udf.py

-- target=log.test_udf
select string_set(array("a", "a", "b")) as stringset
```

Save the file above to a file named `etl_with_udf.sql`, and then run command below to execute the ETL.

```bash
bash -c "$(python3 -m easy_sql.data_process -f etl_with_udf.sql -p)"
```

You'll get a result like:

```
===================== REPORT FOR step-1 ==================
config: StepConfig(target=log.test_udf, condition=None, line_no=5)
sql: select string_set(array("a", "a", "b")) as stringset
status: SUCCEEDED
start time: xxxx-xx-xx xx:xx:xx, end time: xxxx-xx-xx xx:xx:xx, execution time: 2.194115s - 65.29%
messages:
stringset=['a', 'b']
```

## For flink backend

We can create flink UDF/UDAF using scala or python.

It's easy to create a flink UDF. An example is as below:

```scala
class TestFunction extends ScalarFunction {
  def eval(a: Integer, b: Integer): Integer = {
    a + b + 10
  }
}
```

For details, please refer to flink [UDF](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/functions/udfs/) introduction page.

```python
from pyflink.table import DataTypes
from pyflink.table.udf import udf

__all__ = ["test_func"]

@udf(result_type=DataTypes.BIGINT())
def test_func(a: int, b: int) -> int:
    return a + b
```

For details, please refer to pyflink [UDF](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/table/udfs/overview/) introduction page.

### Register and use scala UDF

To define and register a scala UDF, we need to create a scala file with a funciton named `initUdf`.
Below is an example:

```scala
// udfs.scala
package your.company

import org.apache.flink.table.api._
import org.apache.flink.table.functions.ScalarFunction

class TestFunction extends ScalarFunction {
  def eval(a: Integer, b: Integer): Integer = {
    a + b + 10
  }
}

object udfs {
    def initUdfs(flink: TableEnvironment) {
        flink.createTemporarySystemFunction("test_func", classOf[TestFunction])
    }
}
```

The next thing is to compile this file and package it into a jar file. An example command line could be:

```bash
# SCALA_BIN is a path to the bin folder of scala sdk directory.
# SCALA_CP is the java class path for compiling. Usually are some jars.
${SCALA_BIN}/scalac -nobootcp -cp ${SCALA_CP} -d classes your/company/*.scala
cd classes && jar -cvf ../udf.jar .
```

After the command above, you'll get a jar file named `udf.jar`.

We can register the above UDFs in ETL configuration block. Below is an example:

```sql
-- backend: flink
-- config: jarfile=udf.jar
-- config: easy_sql.scala_udf_initializer=your.company.udfs

-- target=log.test_udf
select test_func(1, 2) as sum_value
```

Save the file above to a file named `etl_with_udf.sql`, and then run command below to execute the ETL.

```bash
python3 -m easy_sql.data_process -f etl_with_udf.sql
```

You'll get a result like:

```
===================== REPORT FOR step-1 ==================
config: StepConfig(target=log.test_udf, condition=None, line_no=5)
sql: select test_func(1, 2) as sum_value
status: SUCCEEDED
start time: 2022-09-07 11:14:53, end time: 2022-09-07 11:15:01, execution time: 8.114609s - 99.82%
messages:
sum_value=13
```

### Register and use python UDF

To register a python UDF is much easier. But if you choose to implement UDF in python, there might be a performance issue
since spark application runs in Java and must talk to a python process when calling UDF.

First, we need to define a UDF in a python file:

```python
from pyflink.table import DataTypes
from pyflink.table.udf import udf

__all__ = ["test_func"]

@udf(result_type=DataTypes.BIGINT())
def test_func(a: int, b: int) -> int:
    return a + b
```

Let's assume the file name is `udf.py`.

We can register the above UDFs in ETL configuration block. Below is an example:

```sql
-- backend: flink
-- config: easy_sql.udf_file_path=udf.py

-- target=log.test_udf
select test_func(1, 2) as sum_value
```

Save the file above to a file named `etl_with_udf.sql`, and then run command below to execute the ETL.

```bash
bash -c "$(python3 -m easy_sql.data_process -f etl_with_udf.sql -p)"
```

You'll get a result like:

```
===================== REPORT FOR step-1 ==================
config: StepConfig(target=log.test_udf, condition=None, line_no=4)
sql: select test_func(1, 2) as sum_value
status: SUCCEEDED
start time: 2022-09-07 11:18:45, end time: 2022-09-07 11:18:55, execution time: 9.959185s - 98.94%
messages:
sum_value=3
```

## For other backends

For other backends, we can create UDF/UDAF with SQL.

Easy SQL provides a way to define some SQL to create UDF in a python file.

An example for clickhouse backend is as follows:

```python
def translate():
    return "CREATE FUNCTION IF NOT EXISTS translate AS (input, from, to) -> replaceAll(input, from, to)"
```

Let's assume the file name is `udf.py`.

We can register the above UDFs in ETL configuration block. Below is an example:

```sql
-- backend: clickhouse
-- config: easy_sql.udf_file_path=udf.py

-- target=log.test_udf
select translate('abcad', 'a', '')) as translated_str
```

Save the file above to a file named `etl_with_udf.sql`, and then run command below to execute the ETL.

```bash
CLICKHOUSE_URL=clickhouse+native://default@localhost:9000 python3 -m easy_sql.data_process -f etl_with_udf.sql
```

You'll get a result like:

```
===================== REPORT FOR step-1 ==================
config: StepConfig(target=log.test_udf, condition=None, line_no=4)
sql: select translate('abcad', 'a', '') as translated_str
status: SUCCEEDED
start time: xxxx-xx-xx xx:xx:xx, end time: xxxx-xx-xx xx:xx:xx, execution time: 0.048148s - 70.54%
messages:
(translated_str='bcd')
```

**Notes:**

- We need to follow the syntax provided by the backend to create UDF / UDAF.
  + For PostgreSql, we can refer to the doc [here](https://www.postgresql.org/docs/current/sql-createfunction.html).
  + For Clickhouse, we can refer to the doc [here](https://clickhouse.com/docs/en/sql-reference/statements/create/function/).
  + For BigQuery, we can refer to the doc [here](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions).

## Register and use UDF programmatically

We can register and use UDF in a programmatic manner as well. Below is an example of some sample code:

```python
from pyspark.sql import SparkSession

from easy_sql.sql_processor import SqlProcessor
from easy_sql.sql_processor.backend import SparkBackend

if __name__ == '__main__':
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    backend = SparkBackend(spark)
    sql = '''
-- target=log.test_udf
select string_set(array("a", "a", "b")) as stringset
    '''
    sql_processor = SqlProcessor(backend, sql, scala_udf_initializer='your.company.udfs')
    sql_processor.register_udfs_from_pyfile('udf.py')
    sql_processor.run()
```

For a detailed sample implementation and other backends,
please refer to the implementation of [data_process](https://github.com/easysql/easy_sql/blob/main/easy_sql/data_process.py) module.

## UDF reference

There are several UDFs implemented in Easy SQL. Below are a list of them for referencing.


### Spark UDFs

- [`remove_all_whitespaces(value: str) -> str`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/udf/udfs/index.html#easy_sql.udf.udfs.SparkUdfs.remove_all_whitespaces)
- [`trim_all(value: str) -> str`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/udf/udfs/index.html#easy_sql.udf.udfs.SparkUdfs.trim_all)



### PostgreSQL UDFs

- [`date_format()`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/udf/udfs/index.html#easy_sql.udf.udfs.PgUdfs.date_format)
- [`from_unixtime()`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/udf/udfs/index.html#easy_sql.udf.udfs.PgUdfs.from_unixtime)
- [`get_json_object()`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/udf/udfs/index.html#easy_sql.udf.udfs.PgUdfs.get_json_object)
- [`sha1()`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/udf/udfs/index.html#easy_sql.udf.udfs.PgUdfs.sha1)
- [`split()`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/udf/udfs/index.html#easy_sql.udf.udfs.PgUdfs.split)
- [`trim_all()`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/udf/udfs/index.html#easy_sql.udf.udfs.PgUdfs.trim_all)



### Clickhouse UDFs

- [`translate()`](https://easy-sql.readthedocs.io/en/latest/autoapi/easy_sql/udf/udfs/index.html#easy_sql.udf.udfs.ChUdfs.translate)
