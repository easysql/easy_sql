# Debugging ETL

There is a debugger interface implemented in Easy SQL.

## Start to debug

We recommend debugging ETLs from jupyter. You can follow the steps below to start debugging your ETL.

1. Install jupyter first with command `pip install jupyterlab`.

2. Create a file named `debugger.py` with contents like below:

A more detailed sample could be found [here](https://github.com/easysql/easy_sql/blob/main/debugger.py).

```python
from typing import Dict, Any

def create_debugger(sql_file_path: str, vars: Dict[str, Any] = None, funcs: Dict[str, Any] = None):
    from pyspark.sql import SparkSession
    from easy_sql.sql_processor.backend import SparkBackend
    from easy_sql.sql_processor_debugger import SqlProcessorDebugger
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    backend = SparkBackend(spark)
    debugger = SqlProcessorDebugger(sql_file_path, backend, vars, funcs)
    return debugger

```

3. Create a file named `test.sql` with contents as [here](https://github.com/easysql/easy_sql/blob/main/test/sample_etl.spark.sql).

4. Then start jupyter lab with command: `jupyter lab`.

5. Start debugging like below:

![ETL Debugging](https://raw.githubusercontent.com/easysql/easy_sql/main/debugger-usage.gif)

## Debuger API

Please refer to API doc [here](api/debugger.md)
