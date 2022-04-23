FROM nikolaik/python-nodejs:python3.8-nodejs12

RUN apt-get update && apt-get install -y vim wget openjdk-11-jdk zip unzip lsof less

RUN python3 -m pip install click==6.7 pyspark==3.2.1
RUN pip3 install SQLAlchemy==1.3.23 psycopg2-binary==2.8.6
RUN pip3 install clickhouse-driver==0.2.0 clickhouse-sqlalchemy==0.1.6

WORKDIR /tmp

ADD sample_etl.spark.sql /tmp
ADD sample_etl.postgres.sql /tmp
ADD sample_etl.clickhouse.sql /tmp

RUN python3 -m pip install easy_sql-easy_sql

ARG PG_URL=
ARG CLICKHOUSE_URL=

RUN bash -c "$(python3 -m easy_sql.data_process -f sample_etl.spark.sql -p)"
RUN PG_URL=$PG_URL python3 -m easy_sql.data_process -f sample_etl.postgres.sql
RUN CLICKHOUSE_URL=$CLICKHOUSE_URL python3 -m easy_sql.data_process -f sample_etl.clickhouse.sql
