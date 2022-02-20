package com.bigdata.flink.primary;

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/*
使用较新 的 Table API写法
只需要 bridge planner 两个依赖就够了
官网 1.14.3 （与1.13.5不兼容）
 */
public class HelloTableSQLAPI114 {
    static class CommonStructure{
        public static void main(String[] args) {
            EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            TableEnvironment tableEnv = TableEnvironment.create(settings);
            //创建源表
            Schema schema = Schema.newBuilder()
                    .column("f0", DataTypes.STRING())
                    .build();
            TableDescriptor descriptor = TableDescriptor.forConnector("datagen")
                    .schema(schema)
                    .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
                    .build();
            tableEnv.createTemporaryTable("SourceTable", descriptor);
            //使用SQL DDL 创建 Sink Table
            tableEnv.executeSql("create temporary table SinkTable with ('connector'='blackhole') like SourceTable");
            //从Table API query中创建Table对象
            Table table2 = tableEnv.from("SourceTable");
            //从 SQL query中创建Table对象
            Table table3 = tableEnv.sqlQuery("select * from SourceTable");
            //将Table API处理结果发射到一个TableSink中，跟SQL DDL效果相同
            TableResult tableResult = table2.executeInsert("SinkTable");
        }
    }
    //Table绑定到指定的 TableEnvironment,不可在一个query中对不同TableEnvironment的table进行join 或union操作
    static class CreateATableEnvironment{
        public static void main(String[] args) {
            EnvironmentSettings settings = EnvironmentSettings
                    .newInstance()
                    .useBlinkPlanner() //1.14+不需要再use 哪个 planner了
                    .inStreamingMode()
                    //.inBatchMode()
                    .build();
            TableEnvironment tableEnv = TableEnvironment.create(settings);
            //还可以使用 StreamExecutionEnvironment 创建 StreamTableEnvironment
            StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamTableEnvironment tableStreamEnv = StreamTableEnvironment.create(streamEnv);

            //## 在Catalog中创建Table

        }
    }


}