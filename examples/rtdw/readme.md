## Target

- ingest data: support both snapshot and incremental data
- etl development: join big tables with updates

## TODO
- [√] setup a scenario
- kafka to help ingest data
    + if use flink cdc directly, it will use multiple connections and add pressure to db
    + consider ingest snapshot first and then data change log

## V1

**Solution:**

pg  ----------->  kafka  -------------->  dw
      flink-cdc          spark-streaming


**Prepare environment:**

```bash
# execute the commands below in separate terminals

# prepare data in pg: run workflow/sales/ods/data.sql in pg manually
# start kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
bin/kafka-console-consumer.sh --topic pgcdc --from-beginning --bootstrap-server localhost:9092
# start flink cdc to kafka (in sales/ods)
java -cp '~/dev/sdks/scala-2.12.10/lib/scala-library.jar:easysql-example-ods.jar:/usr/local/lib/python3.8/site-packages/pyflink/lib/*:jars/*' com.easysql.example.PostgresCDC
# start spark streaming app to ingest data to hudi (in workflow)
bash -c "$(python3 -m easy_sql.data_process -f sales/ods/ingest_hudi.sql -p)" 2>&1 | tee ingest_hudi.log
# query the ingested data to hudi (in workflow)
bash -c "$(python3 -m easy_sql.data_process -f sales/ods/ingest_hudi.test.sql -p)"
```

**Test**

Emit queries in postgres.

Cases to test:
- add data: `insert into inventory.product(pid,pname,pprice) values ('6','prodcut-006',225.31);`
- change data: `update inventory.product set pname='p6' where pid=6;`
- delete data: `delete from inventory.product where pid=6;`
- add column with default value: `alter table inventory.product add column ex int default 0;`
- delete column: `alter table inventory.product delete column ex;`
- rename column: `alter table inventory.product rename column ex to ex1;`
- change column type: `alter table inventory.product change column ex ex1 int;`
