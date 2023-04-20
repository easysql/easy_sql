package com.easysql.example;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@Slf4j
public class Ingest {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void ingest(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String jsonOpts) throws Exception {
        val ic = objectMapper.readValue(jsonOpts, IngestConfig.class);
        env.getCheckpointConfig().setCheckpointInterval(10000);
        val cdcSource = Sources.createPgCDCSource(ic.source_connector.options, ic.schema_list, ic.table_list);
        final SplitTableFunction splitTableFunction = new SplitTableFunction(Arrays.asList("inventory.user,inventory.product,inventory.user_order".split(",")));
        val ds = env.addSource(cdcSource)
            .process(splitTableFunction);
        splitTableFunction.getOutputTags().values().forEach(tag -> {
            tEnv.fromDataStream(ds.getSideOutput(tag).map(data -> data).setParallelism(1));
            val tmpTableName = "_tmp__" + tag.getId().replace(".", "__");
            tEnv.createTemporaryView(tmpTableName, ds);
        });
        env.execute();
    }

    @Data
    public static class IngestConfig {

        private List<HashMap<String, String>> catalogs;
        private List<Database> databases;
        private List<String> schema_list;
        private List<String> table_list;
        private Connector source_connector;

        public Database db(String dbName) {
            return databases.stream().filter(db -> dbName.equals(db.getName())).findFirst().orElse(null);
        }

        public HashMap<String, String> connector(String dbName, String connectorName) {
            val db = db(dbName);
            val conn = db.connector(connectorName);
            return conn.getOptions();
        }

        @Data
        public static class Database {

            private String name;
            private List<Connector> connectors;
            private List<Table> tables;

            public Connector connector(String name) {
                return connectors.stream().filter(prop -> name.equals(prop.getName())).findFirst().orElse(null);
            }

            public Table table(String name) {
                return tables.stream().filter(prop -> name.equals(prop.getName())).findFirst().orElse(null);
            }

        }

        @Data
        public static class Connector {

            private String name;
            private HashMap<String, String> options;
        }

        @Data
        public static class Table {

            private String name;
            private Connector connector;
            private List<String> schema;

            public HashMap<String, String> fullOptions(Database db) {
                val dbConn = db.connector(connector.name);
                val result = new HashMap<>(dbConn.getOptions());
                if (connector.getOptions() != null) {
                    result.putAll(connector.getOptions());
                    if (connector.getOptions().containsKey("path")) {
                        result.put("path", String.format("%s/%s.db/%s", result.get("path"), db.name, name));
                    }
                }
                return result;
            }
        }
    }
}
