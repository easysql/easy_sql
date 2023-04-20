package com.easysql.example;

import com.easysql.example.Ingest.IngestConfig;
import com.easysql.example.Ingest.IngestConfig.Database;
import com.easysql.example.Ingest.IngestConfig.Table;
import lombok.val;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import java.util.HashMap;
import java.util.Map;

public class Sinks {
    public static void createHudiSink(String db, String table, IngestConfig config) {
        final Database dbObj = config.db(db);
        final Table tableObj = dbObj.table(table);
        val tableOpts = tableObj.fullOptions(dbObj);
        val tableSchema = tableObj.getSchema();

        HoodiePipeline.Builder builder = HoodiePipeline.builder(table)
            .column("uuid VARCHAR(20)")
            .column("name VARCHAR(10)")
            .column("age INT")
            .column("ts TIMESTAMP(3)")
            .column("_di VARCHAR(20)")
            .partition("_di")
            .options(tableOpts);

//        builder.sink(dataStream, false);
    }
}
