package com.easysql.example;

import com.easysql.example.RowDataDebeziumDeserializationSchema.GenericRowDataWithSchema;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class SplitTableFunction extends ProcessFunction<GenericRowDataWithSchema, GenericRowDataWithSchema> {

    private List<String> tables;
    private transient Map<String, OutputTag<GenericRowDataWithSchema>> outputTags = null;

    public SplitTableFunction(List<String> tables) { this.tables = tables; }

    public Map<String, OutputTag<GenericRowDataWithSchema>> getOutputTags() {
        if (outputTags == null) {
            val dbTables = tables.stream()
                .map(table -> new Tuple2<>(table.substring(0, table.indexOf(".")), table.substring(table.indexOf(".") + 1)))
                .collect(Collectors.toList());
            outputTags = dbTables.stream()
                .map(table -> new OutputTag<GenericRowDataWithSchema>(table.f0 + "." + table.f1) { })
                .collect(Collectors.toMap(OutputTag::getId, Function.identity()));
        }
        return outputTags;
    }

    @Override
    public void processElement(GenericRowDataWithSchema rowData, ProcessFunction<GenericRowDataWithSchema, GenericRowDataWithSchema>.Context ctx, Collector<GenericRowDataWithSchema> out) throws Exception {
        val table = rowData.getTable();
        val tags = this.getOutputTags();
        if (tags.containsKey(table)) {
            val tag = tags.get(table);
            ctx.output(tag, rowData);
        } else {
            log.debug("Ignore message for table {} since it it not configured to process.", table);
        }
    }

}
