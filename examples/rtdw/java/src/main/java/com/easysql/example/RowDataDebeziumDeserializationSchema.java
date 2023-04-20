package com.easysql.example;

import com.easysql.example.RowDataDebeziumDeserializationSchema.GenericRowDataWithSchema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.DebeziumChangelogMode;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.Envelope.Operation;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RowDataDebeziumDeserializationSchema
    implements DebeziumDeserializationSchema<GenericRowDataWithSchema> {

    private final DebeziumChangelogMode changelogMode;

    public RowDataDebeziumDeserializationSchema(DebeziumChangelogMode changelogMode) { this.changelogMode = changelogMode; }

    public RowDataDebeziumDeserializationSchema(String changelogMode) {
        this(toDebeziumChangelogMode(changelogMode));
    }

    private static DebeziumChangelogMode toDebeziumChangelogMode(String changelogMode) {
        switch (changelogMode) {
            case "all":
                return DebeziumChangelogMode.ALL;
            case "upsert":
                return DebeziumChangelogMode.UPSERT;
            default:
                throw new RuntimeException("Unknown changelog.mode. All values are: all, upsert");
        }
    }

    @Override
    public void deserialize(SourceRecord record, Collector<GenericRowDataWithSchema> out) throws Exception {
        toRowData(record, changelogMode).forEach(out::collect);
    }

    @Override
    public TypeInformation<GenericRowDataWithSchema> getProducedType() {
        return TypeInformation.of(GenericRowDataWithSchema.class);
    }


    public List<GenericRowDataWithSchema> toRowData(SourceRecord record, DebeziumChangelogMode changelogMode) throws Exception {
        Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        List<GenericRowDataWithSchema> rowData = new ArrayList<>();
        if (op != Operation.CREATE && op != Operation.READ) {
            if (op == Operation.DELETE) {
                rowData.add(this.extractRow(value, valueSchema, FieldName.BEFORE, RowKind.DELETE));
            } else {
                if (changelogMode == DebeziumChangelogMode.ALL) {
                    rowData.add(this.extractRow(value, valueSchema, FieldName.BEFORE, RowKind.UPDATE_BEFORE));
                }
                rowData.add(this.extractRow(value, valueSchema, FieldName.AFTER, RowKind.UPDATE_AFTER));
            }
        } else {
            rowData.add(this.extractRow(value, valueSchema, FieldName.AFTER, RowKind.INSERT));
        }
        return rowData;
    }

    private GenericRowDataWithSchema extractRow(Struct record, Schema recordSchema, String dataFieldName, RowKind rowKind) throws Exception {
        Schema dataSchema = recordSchema.field(dataFieldName).schema();
        Struct data = record.getStruct(dataFieldName);

        final List<Field> fields = new ArrayList<>(dataSchema.fields());
        val source = record.getStruct(FieldName.SOURCE);
        val table = source.getString("schema") + "." + source.getString("table");

        val ts = source.getInt64(FieldName.TIMESTAMP);
        fields.add(new Field("_op_ts", fields.size(), Schema.INT64_SCHEMA));

        val idxAlignedFields = IntStream.range(0, fields.size()).mapToObj(i -> {
            val field = fields.get(i);
            return new Field(field.name(), i, field.schema());
        }).collect(Collectors.toList());

        val rd = new Object[fields.size()];
        IntStream.range(0, fields.size() - 1).forEach(i -> rd[i] = data.get(fields.get(i)));
        rd[fields.size() - 1] = ts;

        return GenericRowDataWithSchema.from(idxAlignedFields, rd, rowKind, table);
    }

    public enum SerializableRowType {
        INT, BIGINT, FLOAT, DECIMAL, BOOLEAN, STRING, BINARY,
    }

    @Data
    @AllArgsConstructor
    public static class SerializableRowField {

        private String name;
        private SerializableRowType type;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class GenericRowDataWithSchema {

        private List<SerializableRowField> rowType;
        private Object[] rowData;
        private RowKind rowKind;
        private String table;

        public static GenericRowDataWithSchema from(List<Field> fields, Object[] rowData, RowKind rowKind, String table) {
            val rowType = fields.stream()
                .map(field -> new SerializableRowField(field.name(), toSerializableRowType(field.schema())))
                .collect(Collectors.toList());
            return new GenericRowDataWithSchema(rowType, rowData, rowKind, table);
        }

        private static SerializableRowType toSerializableRowType(Schema schema) {
            switch (schema.type()) {
                case INT8:
                case INT16:
                case INT32:
                    return SerializableRowType.INT;
                case INT64:
                    return SerializableRowType.BIGINT;
                case FLOAT32:
                    return SerializableRowType.FLOAT;
                case FLOAT64:
                    return SerializableRowType.DECIMAL;
                case BOOLEAN:
                    return SerializableRowType.BOOLEAN;
                case STRING:
                    return SerializableRowType.STRING;
                case BYTES:
                    return SerializableRowType.BINARY;
                default:
                    throw new RuntimeException("Unsupported type: " + schema.type());
            }
        }

        public Object getObject(int pos) {
            SerializableRowField field = rowType.get(pos);
            Object value;
            switch (field.getType()) {
                case INT:
                    value = rowData[pos] == null ? null : toNumber(rowData[pos], Integer.class);
                    break;
                case BIGINT:
                    value = rowData[pos] == null ? null : toNumber(rowData[pos], Long.class);
                    break;
                case STRING:
                    value = rowData[pos] == null ? null : rowData[pos].toString();
                    break;
                case BOOLEAN:
                    value = rowData[pos] == null ? null : toBoolean(rowData[pos]);
                    break;
                case FLOAT:
                    value = rowData[pos] == null ? null : toNumber(rowData[pos], Float.class);
                    break;
                case DECIMAL:
                    value = rowData[pos] == null ? null : toNumber(rowData[pos], Double.class);
                    break;
                case BINARY:
                    value = rowData[pos] == null ? null : (byte[])rowData[pos];
                    break;
                default:
                    throw new RuntimeException("Unsupported type: " + field.getType());
            }
            return value;
        }

        private Object toBoolean(Object value) {
            if (value instanceof Boolean) {
                return value;
            }
            if (Integer.valueOf(1).equals(value)) {
                return true;
            }
            if (Integer.valueOf(0).equals(value)) {
                return false;
            }
            throw new RuntimeException("Unsupported conversion to boolean from type: " + value.getClass());
        }

        private Number toNumber(Object value, Class clz) {
            if(value instanceof Number) {
                if (clz == Integer.class){
                    return ((Number) value).intValue();
                } else if (clz == Long.class) {
                    return ((Number) value).longValue();
                } else if (clz == Float.class) {
                    return ((Number) value).floatValue();
                }else if (clz == Double.class) {
                    return ((Number) value).doubleValue();
                } else {
                    throw new RuntimeException("Unsupported conversion to class: " + clz);
                }
            } else {
                throw new RuntimeException("Unsupported conversion to integer from class: " + value.getClass());
            }
        }

        public RowKind getKind() {
            return rowKind;
        }
    }

}
