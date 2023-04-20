package com.easysql.example;

import static org.apache.flink.table.api.Expressions.$;

import com.easysql.example.RowDataDebeziumDeserializationSchema.GenericRowDataWithSchema;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Sources {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static Map<String, String> readPgCDC(
        StreamExecutionEnvironment env, StreamTableEnvironment tEnv,
        Map<String, String> pgSourceConfig, String tablesJson) throws JsonProcessingException {
        val tables = Tables.fromJson(tablesJson);
        val cdcSource = Sources.createPgCDCSource(pgSourceConfig, tables.schemaList(), tables.tablePureNameList());

        final SplitTableFunction splitTableFunction = new SplitTableFunction(tables.tableNameList());
        val ds = env.addSource(cdcSource)
            .process(splitTableFunction);

        splitTableFunction.getOutputTags().values().forEach(tag -> {
            final Table table = tables.table(tag.getId());
            val fieldsExpr = new Expression[table.fields.size()];
            IntStream.range(0, fieldsExpr.length).forEach(i -> fieldsExpr[i] = $(table.fields.get(i)));
            val refType = tEnv.toDataStream(tEnv.from(table.getSchemaRefTableName()).select(fieldsExpr)).getType();
            val tableDs = ds.getSideOutput(tag).map(new SelectFields(table.fields, refType)).setParallelism(1);
            tEnv.fromDataStream(tableDs)
                .as(table.fields.get(0), table.fields.subList(1, table.fields.size()).toArray(new String[0]));
            val tmpTableName = tempTableName(tag.getId());
            tEnv.createTemporaryView(tmpTableName, ds);
        });
        return tables.tableNameList().stream()
            .collect(Collectors.toMap(Function.identity(), Sources::tempTableName));
    }

    private static String tempTableName(String table) {
        return "_tmp__" + table.replace(".", "__");
    }

    public static DebeziumSourceFunction<GenericRowDataWithSchema> createPgCDCSource(
        Map<String, String> config, List<String> schemaList, List<String> tableList) {
        val debeziumProps = new Properties();
        val handledKeys = Arrays.asList(
            "hostname", "port", "username", "password", "database-name",
            "slot.name", "decoding.plugin.name", "changelog.mode", "table-list", "schema-list");
        config.forEach((key, value) -> {
            if (!handledKeys.contains(key)) {
                debeziumProps.put(key, value);
            }
        });

        val builder = PostgreSQLSource.<GenericRowDataWithSchema>builder()
            .hostname(config.get("hostname"))
            .port(Integer.parseInt(config.getOrDefault("port", "5432")))
            .username(config.get("username"))
            .password(config.get("password"))
            .database(config.get("database-name"))
            .slotName(config.get("slot.name"))
            .decodingPluginName(config.get("decoding.plugin.name"))
            .debeziumProperties(debeziumProps)
            .deserializer(new RowDataDebeziumDeserializationSchema(config.getOrDefault("changelog.mode", "upsert")));
        if (schemaList != null) {
            builder.schemaList(schemaList.toArray(new String[0]));
        }
        if (tableList != null) {
            builder.tableList(tableList.toArray(new String[0]));
        }
        return builder.build();
    }

    public static class SelectFields implements MapFunction<GenericRowDataWithSchema, Row>, ResultTypeQueryable<Row> {

        private List<String> fields;
        private TypeInformation<Row> rowType;

        public SelectFields(List<String> fields, TypeInformation<Row> rowType) {
            this.fields = fields;
            this.rowType = rowType;
        }

        @Override
        public Row map(GenericRowDataWithSchema rowData) throws Exception {
            val result = new Row(rowData.getKind(), fields.size());
            val rowTypes = rowData.getRowType();
            val fieldIdx = IntStream.range(0, rowTypes.size()).boxed()
                .collect(Collectors.toMap(i -> rowTypes.get(i).getName(), Function.identity()));
            IntStream.range(0, fields.size()).forEach(i -> {
                val fieldName = fields.get(i);
                if (!fieldIdx.containsKey(fieldName)) {
                    result.setField(i, null);
                } else {
                    result.setField(i, rowData.getObject(fieldIdx.get(fieldName)));
                }
            });
            return result;
        }

        @Override
        public TypeInformation<Row> getProducedType() {
            return rowType;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Table {

        private String name;
        private String schemaRefTableName;
        private List<String> fields;

        private String schema() {
            return name.split("\\.")[0];
        }

        private String pureTableName() {
            return name.split("\\.")[1];
        }

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Tables {

        private List<Table> tables;

        private static Tables fromJson(String tablesJson) throws JsonProcessingException {
            return new Tables(objectMapper.readValue(tablesJson, new TypeReference<List<Table>>() { }));
        }

        public List<String> schemaList() {
            return tables.stream().map(Table::schema).distinct().collect(Collectors.toList());
        }

        public List<String> tablePureNameList() {
            return tables.stream().map(Table::pureTableName).distinct().collect(Collectors.toList());
        }

        public List<String> tableNameList() {
            return tables.stream().map(Table::getName).distinct().collect(Collectors.toList());
        }

        public Table table(String tableName) {
            return tables.stream()
                .filter(table -> tableName.equals(table.getName()))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Table not found:" + tableName));
        }
    }

}
