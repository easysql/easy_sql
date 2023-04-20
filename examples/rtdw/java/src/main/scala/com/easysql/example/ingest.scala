package com.easysql.example

import com.ververica.cdc.connectors.postgres.PostgreSQLSource
import com.ververica.cdc.debezium.{DebeziumSourceFunction, JsonDebeziumDeserializationSchema}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import java.util.Properties

object PostgresCDC {
    def createCDCSource(): DebeziumSourceFunction[String]={
        val prop = new Properties()
        prop.setProperty("decimal.handling.mode","string")
        PostgreSQLSource.builder[String]
            .hostname("testpg")
            .port(15432)
            .username("postgres")
            .password("123456")
            .database("postgres")
            .schemaList("inventory")
            .slotName("pg_cdc")
            .decodingPluginName("pgoutput")
            .debeziumProperties(prop)
            .deserializer(new JsonDebeziumDeserializationSchema)
            .build
    }

    def createKafkaSink(): KafkaSink[String] ={
        val sinkTopic = "pgcdc"
        KafkaSink.builder[String].setBootstrapServers("localhost:9092")
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(sinkTopic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
            .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .setTransactionalIdPrefix("pgcdc-transaction-id")
            .setKafkaProducerConfig(Map("transaction.timeout.ms"-> "300000"))
            .build
    }

    implicit def map2Properties(map: Map[String, String]): java.util.Properties = {
        map.foldLeft(new java.util.Properties){ case (props, (k, v)) => props.put(k, v); props }
    }

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.enableCheckpointing(10 * 1000)
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
        env.getCheckpointConfig.setCheckpointTimeout(60000)
        env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        val rocksBackend = new EmbeddedRocksDBStateBackend()
        rocksBackend.setDbStoragePath("/tmp/cdc-flink-states")
        env.setStateBackend(rocksBackend)

        env.addSource(createCDCSource()).name("postgres cdc source")
            .map(data => {
                data
            })
            .setParallelism(1)
//            .print()
            .sinkTo(createKafkaSink()).name("cdc sink kafka")

        env.execute("Postgres CDC")
    }

}
