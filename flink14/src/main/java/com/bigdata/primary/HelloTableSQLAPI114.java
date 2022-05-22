package com.bigdata.primary;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.TumbleWithSizeOnTimeWithAlias;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.FunctionRequirement;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.net.URL;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.CURRENT_RANGE;
import static org.apache.flink.table.api.Expressions.UNBOUNDED_RANGE;
import static org.apache.flink.table.api.Expressions.UNBOUNDED_ROW;
import static org.apache.flink.table.api.Expressions.and;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.concat;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.row;
import static org.apache.flink.table.api.Expressions.rowInterval;

/*
使用较新 的 Table API写法
只需要 bridge planner 两个依赖就够了
官网 1.14.3 （与1.13.5不兼容）
 */
public class HelloTableSQLAPI114 {
    static EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    static TableEnvironment tableEnv = TableEnvironment.create(settings);

    static StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    static StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv);

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
            //在SQL语句中，Table对象对应一个视图VIEW(virtual table)，封装了一个逻辑查询计划
            //1. 简单查询结果作为一张表
            Table projTable = tableEnv.from("X").select(/*...*/);
            //2. 注册projTable 作为 projectedTable
            tableEnv.createTemporaryView("projectedTable",projTable);

            //Connector Tables
            //1. 使用TableDescriptor创建表
            Schema schema = Schema.newBuilder().column("f0", DataTypes.STRING()).build();
            TableDescriptor descriptor = TableDescriptor.forConnector("datagen")
                    .schema(schema).option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
                    .build();
            tableEnv.createTable("SourceTableA", descriptor);
            tableEnv.createTemporaryTable("SourceTableB", descriptor);
            //2. 使用SQL DDL创建表
            tableEnv.executeSql("create [temporary] table MyTable(...) with (...)");

            //扩展Table识别器
            /*
            表总是由3部分identifier注册：catalog,database,tableName
            backtick character (`)
             */
            //声明使用的catalog和database
            tableEnv.useCatalog("custom_catalog");
            tableEnv.useDatabase("custom_database");
            Table table = tableEnv.from("X").select(/*...*/);
            //向custom_catalog custom_database中注册名为 exampleView 的VIEW
            tableEnv.createTemporaryView("exampleView", table);
            //向名为custom_catalog的catalog中注册VIEW - exampleView
            //向名为other_database的database中注册VIEW - exampleView
            tableEnv.createTemporaryView("other_database.exampleView", table);
            tableEnv.createTemporaryView("`example.View`", table); //`符号也可以不加
            tableEnv.createTemporaryView("other_catalog.other_database.exampleView", table);
        }
    }
    /*

     */
    static class QueryATable{
        public static void main(String[] args) {
            Table orders = tableEnv.from("orders");
            //使用Table API
            Table statistics = orders.filter($("country_name").isEqual("China"))
                    .groupBy($("user_id"), $("activity_id"))
                    .select($("user_id"), $("activity_id"), $("price").sum().as("totalCost"));
            //使用 SQL
            Table stats = tableEnv.sqlQuery("select user_id, activity_id, SUM(price) as totalCost " +
                    "from orders where country_name='china' " +
                    "group by user_id,activity_id"
            );

            //从表中查出写入另外的已注册的表
            tableEnv.executeSql("insert into east_china_orders " +
                    "select user_id,activity_id,sum(price) as totalCost " +
                    "from jiangsu_orders " +
                    "where city='nanjing' " +
                    "group by user_id,activity_id"
            );
        }
    }
    /*
        Table处理结果写入TableSink，TableSink可以是文件格式（CSV、Apache Parquet、Apache Avro）、
        存储系统（JDBC、Apache HBase、Apache Cassandra、Elasticsearch）、消息系统（Kafka、RabbitMQ）
        Batch表只能写入BatchTableSink，Stream Table可以写入AppendStreamTableSink、RetractStreamTableSink、UpsertStreamTableSink
        Table.executeInsert(String tableName)方法将表写入一个注册的TableSink中，此方法会使用name和validates从catalog中查找TableSink
        Table的schema与TableSink的schema相同
         */
    static class EmitATable{
        public static void main(String[] args) {
            Schema schema = Schema.newBuilder().column("user_id", DataTypes.INT())
                    .column("activity_id", DataTypes.STRING())
                    .column("order_no", DataTypes.STRING())
                    .build();
            FormatDescriptor format = FormatDescriptor.forFormat("csv")
                    .option("field-delimiter", ",")
                    .build();
            TableDescriptor descriptor = TableDescriptor.forConnector("filesystem")
                    .schema(schema)
                    .option("path", "/path/to/file")
                    .format(format)
                    .build();
            tableEnv.createTemporaryTable("CSVSinkTable", descriptor);
            Table table = tableEnv.sqlQuery("select ... from orders ...");
            table.executeInsert("CSVSinkTable");
        }
    }
    /*
    不论输入数据是stream还是batch的，Table API和SQL查询会被转换成DataStream程序
    查询在内部转换成逻辑查询计划，转换分两阶段：
    - 逻辑计划（logical plan）优化
    - 转换成一个DataStream程序
    转换发生下下述场景：
    - TableEnvironment.executeSql() 方法被调用时，此方法用于执行给定的statement
    - Table.executeInsert() 方法被执行时，此方法用于向给定的sink路径中输出table内容
    - Table.execute() 方法被执行时，此方法用于收集table内容到本地客户端中
    - StatementSet.execute()方法被调用时
        （1）通过StatementSet.addInsert()将数据发送到sink
        （2）通过StatementSet.addInsertSql()方法进行的INSERT statement，会被首先缓存到StatementSet中，StatementSet.execute()方法执行后进行转换
    - Table被转换成DataStream

    查询优化
    Apache Flink使用并且扩展了Apache Calcite来执行查询优化，基本的优化方案如下：
    - 基于Calcite的子查询去相关
    - Project pruning 精简
    - Partition pruning
    - Filter push-down 过滤器下推
    - sub-plan去重，避免重复计算
    - 特殊子查询重写，包括两个部分：
        转换in exists 子查询为 左连接 left semi-joins
        转换not in 和not exists 成 左反连接 left anti-join
    - 可选的联表重排序
        通过参数 table.optimizer.join-reorder-enabled 开启

    可以通过 CalciteConfig 对象进行定制化的优化：TableEnvironment#getConfig#setPlannerConfig
     */
        /*
        Table API提供了一种explain逻辑和优化的查询计划的机制，通过下述两个方法执行：
        - Table.explain() 返回 Table的执行计划
        - StatementSet.explain()方法返回多个sink的执行计划，返回一个字符串描述三个执行计划：
            - 关联查询的抽象语法树，如未经优化的逻辑查询计划
            - 优化的逻辑查询计划
            - 物理执行计划
         */
    static class ExplainATable{
        public static void main(String[] args) {
            StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv);
            DataStreamSource<Tuple2<Integer, String>> source1 = streamEnv.fromElements(Tuple2.of(1, "hello"));
            DataStreamSource<Tuple2<Integer, String>> source2 = streamEnv.fromElements(Tuple2.of(1, "hello"));
            //Table table = streamTableEnv.fromDataStream(source1, $("count"), $("word"));
            Table table = streamTableEnv.fromDataStream(source1,
                    Schema.newBuilder().column("count", DataTypes.INT())
                            .column("word", DataTypes.STRING()).build());
            Table table1 = null;
            Table word = table.where($("word").like("F%"))
                    .unionAll(table1);
            System.out.println(word.explain());
        }
    }
    /*
    多重sink的执行计划要使用StatementSet.explain()方法进行输出
    format 写 csv 会提示 Could not find any format factory for identifier 'csv' in the classpath
    path 随便写也能运行
     */
    static class MultipleSinkExecutePlain{
        public static void main(String[] args) {
            URL resource = MultipleSinkExecutePlain.class.getResource("/");
            String path = resource.getPath();
            System.out.println(path);
            EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
            TableEnvironment tableEnv = TableEnvironment.create(settings);
            Schema schema = Schema.newBuilder()
                    .column("count", DataTypes.INT())
                    .column("word", DataTypes.STRING())
                    .build();
            tableEnv.createTemporaryTable("MySource1",TableDescriptor.forConnector("filesystem")
                    .schema(schema).option("path","source/path1").format("json").build());
            tableEnv.createTemporaryTable("MySource2", TableDescriptor.forConnector("filesystem")
                    .schema(schema).option("path",path+"source/path2").format("json").build());
            tableEnv.createTemporaryTable("MySink1",TableDescriptor.forConnector("filesystem")
                    .schema(schema).option("path",path+"sink/path1").format("json").build());
            tableEnv.createTemporaryTable("MySink2",TableDescriptor.forConnector("filesystem")
                    .schema(schema).option("path",path+"sink/path2").format("json").build());
            StatementSet stmtSet = tableEnv.createStatementSet();
            Table table1 = tableEnv.from("MySource1").where($("word").like("F%"));
            stmtSet.addInsert("MySink1",table1);
            Table table2 = table1.unionAll(tableEnv.from("MySource2"));
            stmtSet.addInsert("MySink2",table2);

            String explain = stmtSet.explain();
            System.out.println(explain);
        }
    }
    /*
    整合DataStream API
    在定义数据处理管道上，TableAPI和DataStream API同等重要
    DataStream API提供了原始的流处理功能，包含time state 数据流管理，但提供的API较为底层
    TableAPI对其进行了抽象，并提供了结构化、声明式的API
    两种API都可以用于有界（历史数据）和无界（实时处理场景，但也可能先通过历史数据初始化）流数据
    两种API都针对有界流数据提供了优化的批处理模式以提升执行效率，因为批处理是流处理的特殊场景，也可以在正常的流处理模式下处理有界流数据
    一种API下的数据可以被定义为端到端不依赖其他类型的API，但混合两种API可以带来许多好处：
    - 访问catalog以使用table生态系统，便捷地连接到外部系统
    - 使用SQL函数处理无状态数据
    - 随时可以转换到DataStreamAPI以便执行一些特定操作，如TableAPI不支持的定时器
    Flink提供了特定的桥接函数平滑实现DataStreamAPI的整合
    > 注意：DataStream 和 Table API之间的转换会带来开销，如table运行时内部的数据结构RowData，需要将二进制数据转换成易读的Row数据结构，
    通常这种开销很小可以进行忽略
     */
        /*
        在DataStream和Table间进行转换
        Flink提供了专门的StreamTableEnvironment与DataStream API进行整合，Table的column的名称和类型可以从DataStream中的TypeInformation中自动生成
        因为DataStream API原生不支持changelog的处理，示例代码中的转换按append-only/insert-only语义进行
         */
    static class GoBackAndForthBetweenDataStreamAndTableAPI{
        public static void main(String[] args) throws Exception{
            DataStreamSource<String> source = streamEnv.fromElements("Alice", "Bob", "John");
            Table table = streamTableEnv.fromDataStream(source);
            //将table对象注册为一个view进行查询
            streamTableEnv.createTemporaryView("InputView", table);
            Table queryedTable = streamTableEnv.sqlQuery("select upper(f0) from InputView");
            //将查到的table转换成DataStream
            DataStream<Row> stream = streamTableEnv.toDataStream(queryedTable);
            stream.print();
            streamEnv.execute("GoBackAndForthBetweenDataStreamAndTableAPI");
                /* 运行结果
                9> +I[JOHN]
                7> +I[ALICE]
                8> +I[BOB]
                 */
        }
    }
    /*

     */
    static class HowToUpdateTables{
        public static void main(String[] args) throws Exception{
            DataStreamSource<Row> dataStream = streamEnv.fromElements(
                    Row.of("alice", 12),
                    Row.of("bob", 10),
                    Row.of("alice", 100));
            Table inputTable = streamTableEnv.fromDataStream(dataStream).as("name", "score");
            streamTableEnv.createTemporaryView("InputTable", inputTable);
            Table resultTable = streamTableEnv.sqlQuery("select name,sum(score) from InputTable group by name");
            //将不断更新中的 Table 转换成 changelog 数据流
            //否则会报 Table sink 'Unregistered_DataStream_Sink_1' doesn't support consuming update changes [...].
            DataStream<Row> resultStream = streamTableEnv.toChangelogStream(resultTable);
            resultStream.print();
            streamEnv.execute();
                /* 打印内容，前面的I表示Insert，后面的U表示Update，通过Row.getKind()可以查询到
                alice的第二个记录会创建两个更新操作 -U 和 +U
                11> +I[bob, 10]
                6> +I[alice, 12]
                6> -U[alice, 12]
                6> +U[alice, 112]
                 */
        }
    }
    static class ProcessBothBatchAndStreamingData{
        public static void main(String[] args) {
            streamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);
            //streamEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            //设置Table API, table environment在初始化时适配 tuntime 模式
            StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv);

                /*
                StreamData API转换成 Table API时，相关设置也会自动进行适配
                这里推荐在转换为Table API之前进行相关设置
                 */
            streamEnv.setParallelism(3);
            //streamEnv.getConfig().addDefaultKryoSerializer();
            streamEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            StreamTableEnvironment streamTableEnv2 = StreamTableEnvironment.create(streamEnv);
            streamTableEnv2.getConfig().setLocalTimeZone(ZoneId.of("Europe/Berlin"));
        }
    }
    /*
    execute 方法在StreamData API和Table API中存在一些差异：
    - StreamData API 中的StreamExecutionEnvironment使用 建造者模式 提供了一个复杂pipeline的创建，
    这个pipeline可能会流向多个分支，最终进入同一个sink，执行环境在job提交前会一直缓存所有定义的分支
    StreamExecutionEnvironment.execute() 方法提交整个构建好的pipeline，清空builder
    通常以 StreamExecutionEnvironment.execute() 方法结尾，但也可以使用 DataStream.executeAndCollect() 隐式定义一个sink将结果导向一个本地客户端
    - 在Table API中，多分支的pipelines 必须通过StatementSet实现，每一个分支需要定义一个sink
    TableEnvironment和StreamTableEnvironment都不提供专用的execute方法，但他们提供了提交单一 source->sink 的pipeline和statement set的途径
     */
    static class SlightDiffInExecutionBehavior{
        public static void main(String[] args) {
            streamTableEnv.from("InputTable").executeInsert("OutputTable");
            streamTableEnv.executeSql("insert into OutputTable select * from InputTable");
            streamTableEnv.createStatementSet()
                    .addInsert("OutputTable", streamTableEnv.from("InputTable"))
                    .addInsert("OutputTable2", streamTableEnv.from("InputTable"))
                    .execute();

            streamTableEnv.createStatementSet()
                    .addInsertSql("insert into OutputTable select * from InputTable")
                    .addInsertSql("insert into OutputTable2 select * from InputTable")
                    .execute();

            streamTableEnv.from("InputTable").execute().print();

            streamTableEnv.executeSql("select * from InputTable").print();
        }
    }
    /*
    通常，数据源的有界属性就是相当于告诉我们，在执行前所有记录是否都是已知的，是否会有新的数据出现。这样对应的job也就具备了有界的属性
    设置运行时环境为Batch需要满足下述要求：
    - 所有数据源必须声明为有界的
    - 当前，table数据源必须emit insert-only 变更
    - 算子需要分配足够的off-heap 内存，以便进行排序和其他中间转换操作
    - 所有表操作必须在batch模式下可用，当前有有一些table操作只支持Stream模式，
     */
    static class BatchRuntimeMode{
        //两种方式创建 Batch 的 Table API
        public static void main(String[] args) {
            streamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);
            StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv);
            //或者
            StreamTableEnvironment batchTableEnv = StreamTableEnvironment.create(streamEnv, EnvironmentSettings.inBatchMode());
        }
    }
    static class UsingDataGenTableSource{
        public static void main(String[] args) throws Exception{
            Schema schema = Schema.newBuilder()
                    .column("uid", DataTypes.TINYINT())
                    .column("payload", DataTypes.STRING())
                    .build();
            //datagen似乎是一个uuid发生器
            TableDescriptor descriptor = TableDescriptor.forConnector("datagen")
                    .option("number-of-rows", "10") //让数据源是有界的
                    .schema(schema).build();
            Table table = streamTableEnv.from(descriptor);

            streamTableEnv.toDataStream(table)
                    .keyBy(new KeySelector<Row, Byte>() {
                        @Override
                        public Byte getKey(Row row) throws Exception {
                            return row.<Byte>getFieldAs("uid");
                        }
                    }).map(new MapFunction<Row, String>() {
                @Override
                public String map(Row row) throws Exception {
                    return "My custom operator: " + row.<String>getFieldAs("payload");
                }
            }).executeAndCollect() //execute() 或者 executeAndCollect()将执行结果输出到local client中
                    .forEachRemaining(System.out::println);
        }
    }
    /*
        两张table进行join后，转成datastream进行处理
     */
    static class ChangeLogUnification{
        public static void main(String[] args) throws Exception{
            streamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);
            StreamTableEnvironment batchTableEnv = StreamTableEnvironment.create(streamEnv);

            String[] userTableCols = {"ts", "uid", "name"};
            TypeInformation<Row> typeInfos = Types.ROW_NAMED(userTableCols, Types.LOCAL_DATE_TIME,
                    TypeInformation.of(Integer.class), BasicTypeInfo.STRING_TYPE_INFO);
            SingleOutputStreamOperator<Row> userStream = streamEnv.fromElements(
                    Row.of(LocalDateTime.parse("2021-08-21T13:00:00"), 1, "Alice"),
                    Row.of(LocalDateTime.parse("2021-08-21T13:05:00"), 2, "Bob"),
                    Row.of(LocalDateTime.parse("2021-08-21T13:10:00"), 2, "Bob")
            ).returns(typeInfos);

            String[] orderTableCols = {"ts", "uid", "amount"};
            TypeInformation<Row> orderTypeInfos = Types.ROW_NAMED(orderTableCols, Types.LOCAL_DATE_TIME, Types.INT, Types.INT);
            SingleOutputStreamOperator<Row> orderStream = streamEnv.fromElements(
                    Row.of(LocalDateTime.parse("2021-08-21T13:02:00"), 1, 122),
                    Row.of(LocalDateTime.parse("2021-08-21T13:07:00"), 2, 239),
                    Row.of(LocalDateTime.parse("2021-08-21T13:11:00"), 2, 999)
            ).returns(orderTypeInfos);

            Schema userSchema = Schema.newBuilder()
                    .column("ts", DataTypes.TIMESTAMP(3))
                    .column("uid", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .watermark("ts", "ts - INTERVAL '1' SECOND")
                    .build();
            batchTableEnv.createTemporaryView("user_table", userStream, userSchema);
            Schema orderSchema = Schema.newBuilder()
                    .column("ts", DataTypes.TIMESTAMP(3))
                    .column("uid", DataTypes.INT())
                    .column("amount", DataTypes.INT())
                    .watermark("ts", "ts - INTERVAL '1' SECOND")
                    .build();
            batchTableEnv.createTemporaryView("order_table", orderStream, orderSchema);

            Table joinedTable = batchTableEnv.sqlQuery(
                    "select u.name, o.amount from user_table u, order_table o " +
                            "where u.uid=o.uid and o.ts between u.ts and u.ts + INTERVAL '5' MINUTES"
            );
            DataStream<Row> joinedStream = batchTableEnv.toDataStream(joinedTable);
            //joinedStream.print();

            joinedStream.keyBy((KeySelector<Row, String>) row -> row.<String>getFieldAs("name"))
                    .process(new KeyedProcessFunction<String, Row, String>() {
                        ValueState<String> seen;

                        @Override
                        public void open(Configuration parameters) throws Exception {
                            seen = getRuntimeContext().getState(
                                    new ValueStateDescriptor<String>("seen",String.class));
                        }
                        @Override
                        public void processElement(Row row, Context context, Collector<String> collector) throws Exception {
                            String name = row.getFieldAs("name");
                            if (seen.value() == null){
                                seen.update(name);
                                collector.collect(name);
                            }
                        }
                    }).print();
            streamEnv.execute();
            /*
                12> Alice
                16> Bob
             */
        }
    }
    /*
    HandlingOfInsertOnlyStreams - 处理insert-only 流数据
      StreamTableEnvironment 提供了下述方法与 DataStream 进行转化
      - fromDataStream(DataStream) 将insert-only的数据变化解析成支持任意类型的table，默认不会传递 Event-time和watermark
      - fromDataStram(DataStream,Schema) 同上，使用Schema定义复杂column字段类型，并支持添加时间属性、水印策略、主键primary key

      - createTemporaryView(String,DataStream)/createTemporaryView(String,fromDataStram(DataStream))
        注册一个stream以便使用SQL进行操作，并提供一个name
      - createTemporaryView(String,DataStream,Schema)/createTemporaryView(String,fromDataStram(DataStream),Schema) 同上

      - toDataStream(Table) 将table转换成insert-only的stream，默认的流记录类型是Row，
        rowtime属性column会被写入DataStream的记录中，watermark也会被传入
      - toDataStream(Table, AbstractDataType) 同上，接收一个转换后的columns类型定义，planner会进行隐式类型转化并对columns重排序来映射输出字段的类型（支持嵌套类型）
      - toDataStream(Table, Class)/toDataStream(Table, DataTypes.of(Class)) 同上，但是通过反射进行快速类型处理

      从Table API的视角来看，从DataStream进行的转换操作类似从一个虚拟table connector中读取和写入：在SQL API中使用CREATE TABLE DDL 进行操作
      虚拟表操作 CREATE TABLE name (schema) WITH (options) statement 中的schema部分可以自动从DataStream类型信息中生成
      虚拟DataStream table connector为每一行数据指定了rowtime字段，该字段元数据如下：
      Key - rowtime; Data Type - TIMESTAMP_LTZ(3) NOT NULL; 描述：Stream流中记录的时间戳; 读写 - 可读可写
      virtual DataStream table source实现了SupportsSourceWatermark,因此可以使用SOURCE_WATERMARK()内置方法兼容DataStream API的watermark策略
     */
    static class FromDataStreamHowto{
        @NoArgsConstructor @AllArgsConstructor
        public static class User{
            public String name;
            public Integer score;
            public Instant event_time;
        }
        public static void main(String[] args) {
            //1. table自动从pojo中获取类型信息
            DataStreamSource<User> streamSource = streamEnv.fromElements(
                    new User("Alice", 4, Instant.ofEpochMilli(1646450444280L)),
                    new User("Bob", 6, Instant.ofEpochMilli(1646450444380L)),
                    new User("Alice", 10, Instant.ofEpochMilli(1646450444480L))
            );
            Table table = streamTableEnv.fromDataStream(streamSource);
            /* table.printSchema();
              `name` STRING,
              `score` INT,
              `event_time` TIMESTAMP_LTZ(9)
             */

            //2. table自动生成columns：增加计算columns（以proctime属性栏为例）
            Schema schema = Schema.newBuilder()
                    .columnByExpression("proc_time", "PROCTIME()")
                    .build();
            Table table1 = streamTableEnv.fromDataStream(streamSource, schema);
            /* table1.printSchema();
            新出现的proc_time字段
            `proc_time` TIMESTAMP_LTZ(3) NOT NULL *PROCTIME* AS PROCTIME()
             */

            //3. 增加watermark策略，创建rowtime属性列
            Schema schema1 = Schema.newBuilder()
                    .columnByExpression("rowtime", "cast(event_time as timestamp_ltz(3))")
                    .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
                    .build();
            Table table2 = streamTableEnv.fromDataStream(streamSource, schema1);
            /* table2.printSchema();
            除去数据字段外的信息
            `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* AS cast(event_time as timestamp_ltz(3)),
            WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS rowtime - INTERVAL '10' SECOND
             */

            //4. source_watermark函数
            Schema schema2 = Schema.newBuilder()
                    .columnByExpression("rowtime", "cast(event_time as timestamp_ltz(3))")
                    .watermark("rowtime", "source_watermark()") //函数写错flink报错会提示支持哪些函数
                    .build();
            Table table3 = streamTableEnv.fromDataStream(streamSource, schema2);
            /* table3.printSchema();
            `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* AS cast(event_time as timestamp_ltz(3)),
            WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS source_watermark()
             */

            //5. 默认的event_time字段精确度是 timestamp_ltz(9) 通过Schema将精确度改为3
            Schema schema3 = Schema.newBuilder()
                    .column("event_time", "timestamp_ltz(3)") //`event_time` TIMESTAMP_LTZ(3) *ROWTIME*,
                    .column("name", "STRING")
                    .column("score", "INT")
                    .watermark("event_time", "source_watermark()")
                    .build();
            Table table4 = streamTableEnv.fromDataStream(streamSource, schema3);
            /* table4.printSchema();
            此案例中 由于insert column 重排序映射的原因，watermark策略没有在schema中显示出来
             */
            /*
            例1 - 无基于时间操作的例子；例4是最常见的流窗口或interval join操作；例2是最常见的processing time使用；
            例5整体依赖user pojo类的声明，可以避免Row的泛型转换；
             */

            //因为DataType提供的信息比TypeInformation丰富，复杂的数据类型推荐使用pojos进行声明
            Schema schema4 = Schema.newBuilder()
                    .column("f0", DataTypes.of(User.class))
                    .build();
            Schema schema5 = Schema.newBuilder()
                    .column("f0", DataTypes.STRUCTURED(
                            User.class,
                            DataTypes.FIELD("name", DataTypes.STRING()),
                            DataTypes.FIELD("score", DataTypes.INT())
                    )).build();
            Table table5 = streamTableEnv.fromDataStream(streamSource, schema4);
        }
    }
    //DataStream可以直接注册成视图View，但只能注册成temporary view，由于其inline/匿名特性，不能在permanent catalog中注册
    // 3 种 从DataStream 创建 TemporaryView 的办法
    static class ExamplesForCreateTemporaryView{
        public static void main(String[] args) throws Exception{
            DataStreamSource<Tuple2<Long, String>> streamSource = streamEnv.fromElements(
                    Tuple2.of(12L, "alice"), Tuple2.of(13L, "bob")
            );
            streamTableEnv.createTemporaryView("MyView", streamSource);
            /* streamTableEnv.from("MyView").printSchema();
                `f0` BIGINT NOT NULL,
                `f1` STRING
             */
            Schema schema = Schema.newBuilder()
                    .column("f0", "BIGINT")
                    .column("f1", "STRING")
                    .build();
            streamTableEnv.createTemporaryView("MyView2",streamSource,schema);//View名称需要唯一，否则 already exists
            /* streamTableEnv.from("MyView2").printSchema();
                `f0` BIGINT,
                `f1` STRING
             */
            streamTableEnv.createTemporaryView("MyView3",
                    streamTableEnv.fromDataStream(streamSource).as("id","name"));
            /*  streamTableEnv.from("MyView3").printSchema();
                `id` BIGINT NOT NULL,
                `name` STRING
             */
        }
    }
    // toDataStream 方法使用的不同场景
    // 注意：只有不再更新的table才支持 toDataStream,
    // 通常基于时间的算子如 窗口、interval join、match_recognize短语 非常适合简单的映射和过滤操作的 insert-only 数据流
    static class ExamplesForToDataStream{
        public static class User{
            public String name;
            public Integer score;
            public Instant event_time;
        }
        public static void main(String[] args) {
            streamTableEnv.executeSql(
                    "create table GeneratedTable (" +
                            "name string,score int,event_time timestamp_ltz(3)," +
                            "watermark for event_time as event_time - interval '10' second" +
                            ") with ('connector'='datagen')");
            Table table = streamTableEnv.from("GeneratedTable");
            //1. 使用默认的转成Row的toDataStream
            //event_time是 rowtime 属性，可以被插入DataStream元数据，watermark策略也被传递过去
            DataStream<Row> dataStream = streamTableEnv.toDataStream(table);
            //2. 提供pojo类
            //planner会进行隐式类型转换和columns重排序，watermark策略也会被传递到DataStream
            DataStream<User> userDataStream = streamTableEnv.toDataStream(table, User.class);
            //3. 使用DataTypes传递数据类型信息
            DataStream<Object> dataStream1 = streamTableEnv.toDataStream(table, DataTypes.STRUCTURED(
                    User.class,
                    DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("score", DataTypes.INT()),
                    DataTypes.FIELD("event_time", DataTypes.TIMESTAMP_LTZ(3))
            ));
        }
    }
    //上述讨论的多是简单的insert-only, 会产生update操作的pipeline需要使用 toChangelogStream
    /*
    flink的table运行环境内部其实是一个changelog处理器，实现了动态table和stream的关联
    StreamTableEnvironment提供了下述方法实现变化数据捕捉(CDC)功能
    - fromChangelogStream(DataStream) 将changelog entries流转换为table，流记录的数据类型必须是flink.types.Row，
        因为RowKind flag在runtime期间被设置，event-time和watermark默认不再传递，此方法应传入RowKind信息，以便确定ChangelogMode
    - fromChangelogStream(DataStream, Schema)
    - fromChangelogStream(DataStream, Schema, ChangelogMode) 完整控制如何将stream转换成changelog
        传入的ChangelogMode帮助planner确定数据类型，是inser-only、upsert还是retract

    - toChangelogStream(Table)  fromChangelogStream(DataStream)的反操作，返回一个数据类型为Row的stream，并为每条记录设置了RowKind
        此方法支持所有类型的更新中的table，如果输入表包含单独的rowtime字段，将被传递到stream数据的timestamp中，watermark策略同样被传递
    - toChangelogStream(Table,Schema,ChangelogMode) 此方法可以完整控制table如何转换到changelog流
    下面给出不同场景下 fromChangelogStream 的用法
     */
    static class ExampleForFromChangelogStream{
        public static void main(String[] args) {
            //1.
            DataStreamSource<Row> streamSource = streamEnv.fromElements(
                    Row.ofKind(RowKind.INSERT, "alice", 12),
                    Row.ofKind(RowKind.INSERT, "bob", 13),
                    Row.ofKind(RowKind.UPDATE_BEFORE, "alice", 12),
                    Row.ofKind(RowKind.UPDATE_AFTER, "alice", 100)
            );
            Table table = streamTableEnv.fromChangelogStream(streamSource);
            streamTableEnv.createTemporaryView("InputTable", table);
            TableResult tableResult = streamTableEnv.executeSql(
                    "select f0 as name,sum(f1) as score from InputTable group by f0");
            /* tableResult.print();
            +----+--------------------------------+-------------+
            | op |                           name |       score |
            +----+--------------------------------+-------------+
            | +I |                            bob |          13 |
            | +I |                          alice |          12 |
            | -D |                          alice |          12 |
            | +I |                          alice |         100 |
            +----+--------------------------------+-------------+
             */

            //2. 去掉 UPDATE_BEFORE 将stream转换为 upsert stream
            //通过减少更新messages的数量，可以高效限制输入变更的种类，使用fromChangelogStream方法传入主键和ChangelogMode信息也可以达到这种效果
            DataStreamSource<Row> streamSource1 = streamEnv.fromElements(
                    Row.ofKind(RowKind.INSERT, "alice", 12),
                    Row.ofKind(RowKind.INSERT, "bob", 5),
                    Row.ofKind(RowKind.INSERT, "bob", 6),
                    Row.ofKind(RowKind.UPDATE_AFTER, "alice", 100)
            );
            Table table1 = streamTableEnv.fromChangelogStream(
                    streamSource1,
                    Schema.newBuilder().primaryKey("f0").build(),
                    ChangelogMode.upsert());
            streamTableEnv.createTemporaryView("InputTable1", table1);
            TableResult tableResult1 = streamTableEnv.executeSql(
                    "select f0 as name, sum(f1) as score from InputTable1 group by f0");//聚合操作
            tableResult1.print();
            /* tableResult1.print();
            +----+--------------------------------+-------------+
            | op |                           name |       score |
            +----+--------------------------------+-------------+
            | +I |                          alice |          12 |
            | -U |                          alice |          12 |
            | +U |                          alice |         100 |
            | +I |                            bob |           5 |
            | -U |                            bob |           5 |
            | +U |                            bob |           6 |
            +----+--------------------------------+-------------+
             */
        }
    }
    //展示不同场景下使用 toChangelogStream 方法
    static class ExamplesForToChangelogStream{
        public static void main(String[] args) throws Exception{
            //1.
            Table table1 = streamTableEnv.fromValues(
                    row("alice", 12), row("alice", 2), row("bob", 12)
            ).as("name", "score")
                    .groupBy($("name"))
                    .select($("name"), $("score").sum());
            streamTableEnv.toChangelogStream(table1)
                    .executeAndCollect()
                    .forEachRemaining(System.out::println);
            /*
            +I[bob, 12]
            +I[alice, 12]
            -U[alice, 12]
            +U[alice, 14]
             */

            //2. 带有event_time的高效转换成DataStream的案例
            //因为event_time是schema中的一个时间属性，默认被设置为stream 记录的timestamp，同时也是Row的一个字段
            streamTableEnv.executeSql("create table GeneratedTable (" +
                    "name string,score int,event_time timestamp_ltz(3)," +
                    "watermark for event_time as event_time - interval '10' second" +
                    ") with ('connector' = 'datagen')");
            Table table = streamTableEnv.from("GeneratedTable");
            /*
            DataStream<Row> dataStream = streamTableEnv.toChangelogStream(table);
            dataStream.process(new ProcessFunction<Row, Void>() {
                @Override
                public void processElement(Row row, Context context, Collector<Void> collector) throws Exception {
                    // prints: [name, score, event_time]
                    System.out.println(row.getFieldNames(true));//会打印大量内容
                    //event_time在row和watermark中都存在
                    assert context.timestamp() == row.<Instant>getFieldAs("event_time").toEpochMilli();
                }
            });
            streamEnv.execute();
             */

            //3. 转换成DataStream，但将时间属性写为元数据列，这样流数据的Row中就没有event_time字段了，这是与案例2的区别
            Schema schema = Schema.newBuilder().column("name", "string")
                    .column("score", "int")
                    .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                    .build();
            DataStream<Row> dataStream1 = streamTableEnv.toChangelogStream(table, schema);
            dataStream1.process(new ProcessFunction<Row, Void>() {
                @Override
                public void processElement(Row row, Context context, Collector<Void> collector) throws Exception {
                    // prints: [name, score]
                    System.out.println(row.getFieldNames(true));
                    //timestamp 只存在于 watermark中
                    // System.out.println(row.<Instant>getFieldAs("event_time")); 报错：Unknown field name 'event_time' for mapping to a position.
                    System.out.println(context.timestamp());
                }
            });
            streamEnv.execute();

            //4. 高阶用户可以调用内部API定义数据结构。不建议这么做以免提升复杂性和增加额外的类型转换
            //转换 timestamp_ltz列到Long/String
            Schema schema1 = Schema.newBuilder().column("name", DataTypes.STRING().bridgedTo(StringData.class))
                    .column("score", DataTypes.INT())
                    .column("event_time", DataTypes.TIMESTAMP_LTZ(3).bridgedTo(Long.class))
                    .build();
            streamTableEnv.toChangelogStream(table, schema1);
            //toChangelogStream(Table).executeAndCollect() 等同于 Table.execute().collect()
        }
    }
    /*
    单个Flink job可以将多个互补关联的pipeline连接到一起依次执行
    source->sink pipeline定义在TableAPI中，可以转换成DataStream
    source既可以是Table，也可以是DataStream再转成Table,所以可以对DataStream API程序使用Table sink
    这个功能的实现可以通过StreamStatementSet，（StreamTableEnvironment.createStatementSet()）planner会优化所有添加到Statement的操作
    下述案例：如何在一个job中将Table程序添加到DataStream API 程序
     */
    static class AddingTableAPIPipeline2DataStreamAPI{
        public static void main(String[] args) throws Exception{
            StreamStatementSet stmtSet = streamTableEnv.createStatementSet();
            Schema schema = Schema.newBuilder()
                    .column("myCol", DataTypes.INT())
                    .column("myOtherCol", DataTypes.BOOLEAN())
                    .build();
            //定义 table from source
            TableDescriptor sourceDescriptor = TableDescriptor.forConnector("datagen")
                    .option("number-of-rows", "30")
                    .schema(schema).build();

            //定义 table sink
            TableDescriptor sinkDescriptor = TableDescriptor.forConnector("print").build();
            Table tableFromSource = streamTableEnv.from(sourceDescriptor);
            stmtSet.addInsert(sinkDescriptor, tableFromSource);

            //定义table from DataStream
            DataStreamSource<Long> streamSource = streamEnv.fromSequence(1, 10);
            Table tableFromStream = streamTableEnv.fromDataStream(streamSource);
            stmtSet.addInsert(sinkDescriptor, tableFromStream);
            //将两个pipeline连接到StreamExecutionEnvironment，执行此方法后statement set会被清除
            stmtSet.attachAsDataStream();

            //其他干扰数据，被丢弃，不会打印出来
            streamEnv.fromElements(40,50,60).addSink(new DiscardingSink<>());
            //打印了一堆随机数
            streamEnv.execute();
        }
    }
    /*
    flink.table.types.DataType 与 flink.api.common.typeinfo.TypeInformation
    DataType额外包含了逻辑SQL的类型细节信息，比TypeInformation要丰富些，在类型转换时可以提供更多的细节信息
    Table 的 columns 名字和类型自动从DataStream的TypeInformation中产生，通过DataStream.getType()方法可以进行类型检查
    建议使用 DataTypes.of(TypeInformation) 进行类型转换
     */

    /*-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= Table API 介绍 =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
    static class AboutTableAPI{
        // fromValues 凭空生成一张表
        static void test1(){
            Table table = streamTableEnv.fromValues(
                row(1, "jack", 12, "america"),
                row(2, "lucy", 13, "england")
            );
            table.printSchema();
            /*
              `f0` INT NOT NULL,
              `f1` CHAR(4) NOT NULL,
              `f2` INT NOT NULL,
              `f3` CHAR(7) NOT NULL
            * */
        }
        //fromValues 方法可以指定字段类型
        static void test2(){
            Table table = streamTableEnv.fromValues(
                DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                    DataTypes.FIELD("name", DataTypes.STRING())
                ),
                row(1, "ABC"),
                row(2L, "ABCDE")
            );
            table.printSchema();
            /*
            `id` DECIMAL(10, 2),
            `name` STRING
             */
        }
        /*-=-=-=-=-=-=-=-=-=-=-=-= 表的聚合操作 Aggregation =-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
        static void test3(){
            Table tOrderInfo = streamTableEnv.from("order_info");
            //select * 怎么写
            Table result = tOrderInfo.select($("*"));
            //对表中的所有字段进行重命名
            Table renameResult = tOrderInfo.as("user_id, order_no, create_time");
            //where 与 filter 用法相同，含义相同
            Table orderByUserIdResult = tOrderInfo.where($("user_id").isEqual("120003"));

            /*-=-=-=-=-=-= 表 的 列操作 =-=-=-=*/
            //如果已经存在同名字段会报错
            Table mainOrderInfo = tOrderInfo.addColumns(concat("main_", $("order_no")));
            //如果存在同名字段不报错，直接覆盖 ???
            tOrderInfo.addOrReplaceColumns(concat("main_", $("order_no")).as("main_order_num"));
            //删除列
            Table table = tOrderInfo.dropColumns($("b"), $("c"));
            //重命名列
            tOrderInfo.renameColumns($("b").as("b2"), $("c").as("c2"));

            /* -=-=-=-=-=- 聚合操作 -=-=-=-=-=-=- */
            //窗口 group by 聚合操作
            Table groupByWindowTable = tOrderInfo
                    .window(Tumble.over(lit(5).minutes()).on($("rowtime")).as("w"))
                    .groupBy($("a"), $("w")) //指定窗口后要按窗口分组，a在前还是w在前？？？
                    .select($("a"), $("w").start(), $("w").end(), $("w").rowtime());
            //窗口 over 聚合操作
            Table overWindowTable = tOrderInfo
                    .window(
                            Over.partitionBy($("user_id")).orderBy($("rowtime"))
                                    .preceding(UNBOUNDED_RANGE).following(CURRENT_RANGE).as("w")
                    ).select($("user_id"), $("price").sum().over($("w")));
            //去重聚合
            //distinct 可以用于 groupby 聚合、 groupby 窗口聚合 和 Over window聚合
            // -- 1
            Table groupByDistinctResult = tOrderInfo.groupBy($("a"))
                    .select($("a"), $("b").distinct().sum().as("d"));
            // -- 2
            TumbleWithSizeOnTimeWithAlias window = Tumble.over(lit(5).minutes()).on($("rowtime")).as("w");
            Table groupByWindowDistinctResult = tOrderInfo.window(window)
                    .groupBy($("a"), $("w"))
                    .select($("a"), $("b").distinct().sum().as("d"));
            // -- 3
            OverWindow overWindow = Over.partitionBy($("a")).orderBy($("rowtime")).preceding(UNBOUNDED_RANGE).as("w");
            Table distinctOverWindow = tOrderInfo.window(overWindow)
                    .select(
                            $("a"),
                            $("b").distinct().avg().over($("w")),
                            $("b").max().over($("w")),
                            $("b").min().over($("w"))
                    );
            // -- 4 使用用户自定义的聚合函数
            streamTableEnv.registerFunction("myAggFunc", new AggregateFunction<Object, Object>() {
                @Override
                public Object getValue(Object o) {
                    return null;
                }
                @Override
                public Object createAccumulator() {
                    return null;
                }
            });
            tOrderInfo.groupBy($("activity_no"))
                    .select(
                            $("activity_no"),
                            call("myAggFunc", $("points")).distinct().as("userDefiniedDistinctAggResult")
                    );
            //registerFunction 方法已过期，要使用新方法
            streamTableEnv.createTemporarySystemFunction("name", new UserDefinedFunction() {
                @Override
                public TypeInference getTypeInference(DataTypeFactory dataTypeFactory) {
                    return null;
                }
                @Override
                public FunctionKind getKind() {
                    return FunctionKind.AGGREGATE;
                }
            });
        }

        /*-=-=-=-=-=-=-=-=-=-=-=-= 表的连接操作 Join =-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
        static void test4(){
            Table left = streamTableEnv.from("atable").select($("a"), $("b"), $("c"));
            Table right = streamTableEnv.from("btable").select($("d"), $("e"), $("f"));

            //Inner Join
            Table result = left.join(right)
                    .where($("a").isEqual($("d")))
                    .select($("a"), $("b"), $("e"));
            //Outer Join
            left.leftOuterJoin(right, $("a").isEqual($("d")))
                    .select($("a"), $("c"), $("e"));
            left.rightOuterJoin(right, $("a").isEqual($("d")));
            left.fullOuterJoin(right, $("a").isEqual($("d")));

            //Interval Join  类似SQL中的 join on xx=xx and ??=??, join语句中有筛选条件
            Table intervalJoinTable = left.join(right).where(
                    and(
                            $("a").isEqual($("d")),
                            $("ltime").isGreaterOrEqual($("rtime").minus(lit(5).minutes())),
                            $("ltime").isLess($("rtime").plus(lit(10).minutes()))
                    )
            ).select($("a"), $("ltime"));

            //Inner Join with table UDF 如果UDF计算结果为null，left表的记录会被丢弃
            left.joinLateral(call("tudf", $("a")).as("s","t","v"))
                    .select($("a"), $("t"));//还有 leftOuterJoinLateral
            //用于跟踪数据变化的临时表 Temporal Table 目前只支持 inner joins
        }

        /*-=-=-=-=-=-=-=-=-=-=-=-= 表的集合操作 Join =-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
        static void test5(){
            Table leftTable = streamTableEnv.from("aCTable");
            Table rightTable = streamTableEnv.from("aCTable");
            //union
            leftTable.unionAll(rightTable);
            //intersect 返回的表中无重复记录，两张表的字段类型必须相同
            leftTable.intersect(rightTable);
            //intersectAll 结果表中存在重复记录，两张表的字段类型必须相同
            leftTable.intersectAll(rightTable);
            // minus 类似 SQL的 except， 在left不在right的记录，left表中存在重复记录会被去重。两张表的字段类型必须相同
            leftTable.minus(rightTable);
            //minusAll 类似 SQL 的 except all, 与minus相同，除了结果表中存在重复记录，left表中有记录a重复n次，right表中记录a重复m次，则返回结果重复 (n-m) 次
            //in 操作: 字段a在right表中必须存在且类型与left表的中的相同
            leftTable.select($("a"), $("b")).where($("a").in(rightTable));
        }
        /*-=-=-=-=-=-=-=-=-=-=-=-= 表的其他操作 OrderBy Offset Fetch =-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
        static void test6(){
            Table leftTable = streamTableEnv.from("aCTable");
            Table rightTable = streamTableEnv.from("aCTable");
            //orderby 在不同分区间进行排序，对于无界流排序的字段应该使用时间戳
            leftTable.orderBy($("a").asc());
            //offset&fetch 相当于SQL的 limit offset
            leftTable.where($("a").isEqual("1330")).fetch(5);
            // -- 跳过 5条取后面5条
            leftTable.where($("a").isEqual("1330")).offset(5).fetch(5);

            //executeInsert 一张表的记录插入另一张表
            leftTable.executeInsert("out_table");
        }
        /*-=-=-=-=-=-=-=-=-=-=-=-= 表的窗口操作 Group Windows =-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
        static void test7(){
            Table table = streamTableEnv.from("Table");
            table.window(Tumble.over(lit(90).seconds()).on($("start_time")).as("w"))
                    //.groupBy($("w"))  // 仅仅对window进行groupBy 只能在一个单一非并行的任务中
                    .groupBy($("w"), $("user_id")) //并行的任务还要依据额外的一个属性进行 groupBy
                    //窗口使用别名w指定，可以查询start end属性，窗口的start end时间是左闭右开的，如一个30分钟的窗口，start=14:00:00.000,rowtime=14:29:59.999,end=14:30:00.000
                    .select($("..."), $("w").start(), $("w").end(), $("w").rowtime());
            //Table API支持的窗口类型
            // -- Tumble
            table.window(Tumble.over(lit(90).seconds()).on($("rowtime")).as($("w")));
            table.window(Tumble.over(lit(90).seconds()).on($("proctime")).as($("w")));
            table.window(Tumble.over(rowInterval(100L)).on($("proctime")).as($("w")));//固定元素数量触发窗口

            //-- Slide 要定义windowSize和slideSize，对于一个15分钟窗口长度，滑动长度5分钟的窗口，一个元素会被窗口触发 3 次，分别属于3个不同的窗口
            table.window(
                    Slide.over(lit(15).minutes()).every(lit(5).minutes())
                            .on($("rowTime_attr")).as("w")
            );
            table.window(
                    Slide.over(lit(15).minutes()).every(lit(5).minutes())
                            .on($("proctime")).as("w")
            );
            table.window(Slide.over(rowInterval(100L)).every(rowInterval(50L)).on($("rowTime_attr")).as("w"));
            // -- Session 窗口没有固定大小
            table.window(Session.withGap(lit(90).seconds()).on($("rowtime")).as("w"));
        }
        /*-=-=-=-=-=-=-=-=-=-=-=-= 表的窗口操作 Over Windows =-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
        /*
        OverWindow 定义了要进行聚合计算的row的范围，范围的指定可以使用时间间隔或记录数量，
        partition by(可选) 每个分区各自排序，聚合函数应用在每个分区上。在流式环境中，如果没有partitionBy将会按非并行的方式运行任务
        order by(必选) 对于流式数据，排序字段必须是eventtime或proctime属性，目前只支持一个排序字段
        preceding(可选) 以当前记录向前找 n个/m个时间长度，
            有界的OverWindow可以指定一个确定的数量或时间长度，无界的OverWindow只能使用常量指定 UNBOUNDED_RANGE、UNBOUNDED_ROW
            不写preceding，也会默认使用 UNBOUNDED_RANGE、UNBOUNDED_ROW
        following(可选) 定义包含在窗口中的记录及其后面的记录，所使用的的单位必须与preceding保持一致（要么都是时间，要么都是数量）
            目前OverWindow的rows following还不支持，只能使用两个常量指定 CURRENT_ROW CURRENT_RANGE
        as(必选) 指定一个OverWindow的别名好在select方法中调用，select中使用的聚合函数只能对select所在的同一个window中进行计算
         */
        static void test8(){
            Table table = streamTableEnv.from("table");
            //无界的Over窗口
            table.window(Over.partitionBy($("user_id")).orderBy($("create_time")).preceding(UNBOUNDED_RANGE).as("w"));
            table.window(Over.partitionBy($("user_id")).orderBy($("proctime").desc()).preceding(UNBOUNDED_RANGE).as("w"));
            table.window(Over.partitionBy($("user_id")).orderBy($("create_time")).preceding(UNBOUNDED_ROW).as("w"));
            //有界的Over窗口
            table.window(Over.partitionBy($("user_id")).orderBy($("rowtime")).preceding(lit(1).minutes()).as("w"));
            table.window(Over.partitionBy($("user_id")).orderBy($("proctime")).preceding(rowInterval(10L)).as("w"));
        }
        /*-=-=-=-=-=-=-=-=-=-=-=-= 基于行的操作 =-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
        public static class MyMapFunction extends ScalarFunction{
            public Row eval(String a){
                return Row.of(a, "pre-" + a);
            }
            @Override
            public TypeInformation<?> getResultType(Class<?>[] signature) {
                return Types.ROW(Types.STRING, Types.STRING);
            }
        }
        public static class MyFlatMapFunction extends TableFunction<Row>{
            public void eval(String str){
                if (str.contains("#")){
                    String[] splits = str.split("#");
                    for (String split : splits){
                        collect(Row.of(split, split.length()));
                    }
                }
            }
            @Override
            public TypeInformation<Row> getResultType() {
                return Types.ROW(Types.STRING, Types.INT);
            }
        }
        public static class MyMinMaxAccumulator {
            public int min=0;
            public int max=0;
        }
        public static class MyMinMaxAgg extends AggregateFunction<Row, MyMinMaxAccumulator>{
            public void accumulate(MyMinMaxAccumulator accumulator, int val){
                if (val < accumulator.min){
                    accumulator.min = val;
                }
                if (val > accumulator.max){
                    accumulator.max = val;
                }
            }
            @Override
            public Row getValue(MyMinMaxAccumulator accumulator) {
                return Row.of(accumulator.min, accumulator.max);
            }
            @Override
            public MyMinMaxAccumulator createAccumulator() {
                return new MyMinMaxAccumulator();
            }
            @Override
            public TypeInformation<Row> getResultType() {
                return new RowTypeInfo(Types.INT, Types.INT);
            }
        }
        public static class Top2Accumulator{
            public Integer first;
            public Integer second;
        }
        public static class Top2AggFunc extends TableAggregateFunction<Tuple2<Integer,Integer>, Top2Accumulator>{
            @Override
            public Top2Accumulator createAccumulator() {
                Top2Accumulator accumulator = new Top2Accumulator();
                accumulator.first = Integer.MIN_VALUE;
                accumulator.second = Integer.MIN_VALUE;
                return accumulator;
            }
            public void emitValue(Top2Accumulator accumulator, Collector<Tuple2<Integer,Integer>> out){
                if (accumulator.first != Integer.MIN_VALUE){
                    out.collect(Tuple2.of(accumulator.first, 1));
                }
                if (accumulator.second != Integer.MIN_VALUE){
                    out.collect(Tuple2.of(accumulator.second, 2));
                }
            }
        }
        static void test9(){
            Table input = streamTableEnv.from("table");
            //使用Map UDF
            ScalarFunction func = new MyMapFunction();
            streamTableEnv.registerFunction("mapFunc", func);
            Table mapTable = input.map(call("mapFunc", $("c"))).as("a", "b");

            //使用FlatMap UDF
            MyFlatMapFunction myFlatMapFunction = new MyFlatMapFunction();
            streamTableEnv.registerFunction("flatMapFunc", myFlatMapFunction);
            Table flatMapTable = input.flatMap(call("flatMapFunc", $("c"))).as("a", "b");

            //Aggregate函数
            MyMinMaxAgg aggregator = new MyMinMaxAgg();
            streamTableEnv.registerFunction("myAggFunc", aggregator);
            Table myAggTable = input.groupBy($("key"))
                    .aggregate(call("myAggFunc", $("a")).as("x", "y"))
                    .select($("key"), $("x"), $("y"));

            //Group Window Aggregate 窗口聚合函数
            Table groupWindowAggTable = input.window(Tumble.over(lit(5).minutes()).on($("rowtime")).as("w"))
                    .groupBy($("key"), $("w"))
                    .aggregate(call("myAggFunc", $("a")).as("x", "y"))
                    //窗口的聚合函数必须使用select来结束，并且select中不支持 $("*")
                    .select($("key"), $("x"), $("w").start());

            //Table Group Window, AggregateFunction 跟 TableAggregateFunction的区别是：后者可能返回0或。。。
            Table tableGroupWindowAggTable = input.groupBy($("key"))
                    .flatAggregate(
                            call("top2", $("a")).as("v", "rank")
                    ).select($("key"), $("v"), $("rank"));
        }
        public static void main(String[] args) {
            test2();
        }
    }

    /*-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= SQL API 介绍 =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
    static class AboutSQLAPI{
        static void test1(){
            //从外部数据源接入数据流
            DataStreamSource<Tuple3<Long,String,Integer>> dataSource = streamEnv.addSource(null);

            //1. flink 管这种非注册好的表叫内联表(inlined table)
            Table inlineTable = streamTableEnv.fromDataStream(dataSource, $("user"), $("product"), $("amount"));
            streamTableEnv.sqlQuery("select sum(amount) from" + inlineTable + "where product like '%橡皮%'");//sql中的表名可以通过Table#toString获得

            //2. 将表注册好再查询
            streamTableEnv.createTemporaryView("order_info", dataSource, $("user_id"), $("product"), $("amount"));
            streamTableEnv.sqlQuery("select product,amount from order_info where product like '%橡皮%'");
        }
        //对生成的表的内容进行查询
        static void test2(){
            streamTableEnv.executeSql("create table order_info(`user_id` bigint,`product` string, `amount` int)with(...)");

            TableResult tableResult1 = streamTableEnv.executeSql("select * from order_info");
            try(CloseableIterator<Row> iterator = tableResult1.collect()) {//使用 try-with-resources 语法取保iterator自动关闭
                while (iterator.hasNext()){
                    final Row next = iterator.next();
                }
            }catch (Exception e) {
                e.printStackTrace();
            }

            final TableResult tableResult2 = streamTableEnv.sqlQuery("select * from order_info").execute();
            tableResult2.print();
        }

        //单条执行和批量执行insert
        //如果一次想执行多个sink表的写入，就需要使用StreamStatementSet 进行多个 addInsertSql
        static void test3(){
            //注册 源表 Orders 和 结果存储表 RubberOrders
            streamTableEnv.executeSql("create table Orders(user bigint, product varchar, amount int) with (...)");
            streamTableEnv.executeSql("create table RubberOrders(product varchar, amount int) with (...)");
            //执行 源表 --> 结果表任务
            TableResult tableResult = streamTableEnv.executeSql("insert into RubberOrders select product,amount from Orders where product like '%Rubber%'");
            //从TableResult中获取JobStatus
            System.out.println(tableResult.getJobClient().get().getJobStatus());

            //如果一次想执行多个sink表的写入，就需要使用StreamStatementSet 进行多个 addInsertSql
            //注册名为 GlassOrders 的 sink table
            streamTableEnv.executeSql("create table GlassOrders(product varchar, amount int) with (...)");
            StreamStatementSet stmtSet = streamTableEnv.createStatementSet();
            stmtSet.addInsertSql("insert into RubberOrders select product,amount from Orders where product like '%Rubber%'");
            stmtSet.addInsertSql("insert into GlassOrders select product,amount from Orders where product like '%Glass%'");
            TableResult execute = stmtSet.execute();
            System.out.println(execute.getJobClient().get().getJobStatus());

            //使用 desc 语法描述表
            streamTableEnv.executeSql("describe Orders").print();
            streamTableEnv.executeSql("desc Orders").print();

            //explain语法
            String sql = "select * from Orders where product like '%Glass%'" +
                    "union all " +
                    "select * form RubberOrders where amount > 10";
            streamTableEnv.executeSql("explain PLAN for " + sql).print();
            streamTableEnv.executeSql("explain ESTIMATED_COST, CHANGELOG_MODE, JSON_EXECUTION_PLAN for " + sql).print();
        }

        static void test4(){
            //使用 use 语句 选择catalog
            streamTableEnv.executeSql("create catalog cat1 with (...)");
            streamTableEnv.executeSql("show catalogs").print();
            // +-----------------+
            // |    catalog name |
            // +-----------------+
            // | default_catalog |
            // | cat1            |
            // +-----------------+
            streamTableEnv.executeSql("use catalog cat1");
            streamTableEnv.executeSql("show databases").print();
            streamTableEnv.executeSql("create databases db1 with (...)");
            streamTableEnv.executeSql("shwo databases").print();
            // 选择数据库（默认 default_database）
            streamTableEnv.executeSql("use db1");
            // 选择模块 (默认 core)
            streamTableEnv.executeSql("use modules hive");
            streamTableEnv.executeSql("show full modules").print();
            // +-------------+-------+
            // | module name |  used |
            // +-------------+-------+
            // |        hive |  true |
            // |        core | false |
            // +-------------+-------+

            // 使用load语句加载 module
            streamTableEnv.executeSql("load module hive with ('hive-version'='3.1.2')");
            streamTableEnv.executeSql("show modules");
            // 使用unload语句卸载 module
            streamTableEnv.executeSql("unload module core");
        }

        //UDF函数定义
        public static class SubstrFunction extends ScalarFunction{
            public String eval(String str, Integer begin, Integer end){
                return str.substring(begin, end);
            }
        }
        //带有字段属性的UDF函数
        public static class SubstringFunction extends ScalarFunction{
            private boolean endInclusive;
            public SubstringFunction(boolean endInclusive){
                this.endInclusive = endInclusive;
            }
            public String eval(String s, Integer begin, Integer end){
                return s.substring(begin, endInclusive ? end+1 : end);
            }
        }
        public static class MyConcatFunction extends ScalarFunction{
            public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... fields){
                return Arrays.stream(fields).map(Object::toString).collect(Collectors.joining(","));
            }
        }
        //全局定义UDF输出类型
        @FunctionHint(output = @DataTypeHint("ROW<s STRING, i INT>"))
        public static class OverloadedFunction extends TableFunction<Row>{
            public void eval(int a, int b){
                collect(Row.of("Sum", a+b));
            }
            public void eval(){
                collect(Row.of("Empty args", -1));
            }
        }
        //完全由 FunctionHint 提供类型信息
        @FunctionHint(
                input = {@DataTypeHint("INT"), @DataTypeHint("INT")},
                output = @DataTypeHint("INT")
        )
        @FunctionHint(
                input = {@DataTypeHint("BIGINT"), @DataTypeHint("BINGINT")},
                output = @DataTypeHint("BIGINT")
        )
        @FunctionHint(input = {}, output = @DataTypeHint("BOOLEAN"))
        //@FunctionHints()
        public static class OverloadedFunction2 extends TableFunction<Object>{
            public void eval(Object... o){
                if (o.length == 0){
                    collect(false);
                }
                collect(o[0]);
            }
        }
        //高阶用法：覆写 getTypeInference 方法实现更灵活的类型解析
        public static class LiteralFunction extends ScalarFunction{
            public Object eval(String s, String type){
                switch (type){
                    case "INT":
                        return Integer.valueOf(s);
                    case "DOUBLE":
                        return Double.valueOf(s);
                    case "STRING":
                    default:
                        return s;
                }
            }
            @Override
            public TypeInference getTypeInference(DataTypeFactory typeFactory) {
                return TypeInference.newBuilder()
                        .typedArguments(DataTypes.STRING(), DataTypes.STRING())
                        .outputTypeStrategy(callContext -> {
                            if (!callContext.isArgumentLiteral(1) || callContext.isArgumentNull(1)){
                                throw callContext.newValidationError("params missed!");
                            }
                            String literal = callContext.getArgumentValue(1, String.class).orElse("STRING");
                            switch (literal){
                                case "INT":
                                    return Optional.of(DataTypes.INT().notNull());
                                case "DOUBLE":
                                    return Optional.of(DataTypes.DOUBLE().notNull());
                                case "STRING":
                                default:
                                    return Optional.of(DataTypes.STRING());
                            }
                        }).build();
            }
        }
        public static class HashCodeFunction extends ScalarFunction{
            private int factor = 0;
            @Override
            public void open(FunctionContext context) throws Exception {
                //访问job参数 hashcode_factor ，12是默认值
                factor = Integer.parseInt(context.getJobParameter("hashcode_factor", "12"));
            }
            public int eval(String s){
                return s.hashCode()*factor;
            }
        }

        static void testUDF(){
            //不注册直接使用Table API调用inline函数
            streamTableEnv.from("MyTable").select(call(SubstrFunction.class, $("myField"), 5, 12));
            //注册函数
            streamTableEnv.createTemporarySystemFunction("SubstrFunction", SubstrFunction.class);
            //使用Table API调用上面注册的函数
            streamTableEnv.from("MyTable").select(call("SubstrFunction", $("myField"), 5, 12));
            //使用SQL调用上面注册的函数
            streamTableEnv.sqlQuery("select SubstrFunction(myField,5,12) from MyTable");

            //inline方式调用
            streamTableEnv.from("MyTable").select(call(new SubstringFunction(true), $("myField"), 5,12));
            streamTableEnv.createTemporarySystemFunction("SubstringFunction", new SubstringFunction(true));

            //UDF函数的入参如果是可变参数 Object ...,可以在Table API中通过通配符*一次传入全部的columns
            streamTableEnv.from("MyTable").select(call(MyConcatFunction.class, $("*")));
            //相当于专门指定所有的列
            streamTableEnv.from("MyTable").select(call(MyConcatFunction.class, $("a"), $("b"), $("c")));

            //UDF 函数在open方法中处理job参数
            //hashCode计算函数的一个参数通过 flink job 参数传入
            streamTableEnv.getConfig().addJobParameter("hashcode_factor","31");
            streamTableEnv.createTemporarySystemFunction("hashCode", HashCodeFunction.class);
            streamTableEnv.sqlQuery("select myField,hashCode(myField) from MyTable");
        }

        /*-=-=-=-=-=-=-=-=除了UDF还有一类函数叫 UDTF user defined table function -=-=-=-=-=-=-=-=-*/
        @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
        public static class SplitFunction extends TableFunction<Row>{
            public void eval(String str){
                for(String s : str.split(" ")){
                    collect(Row.of(s, s.length()));
                }
            }
        }
        static void testUDTF(){
            streamTableEnv.from("MyTable")
                    .joinLateral(call(SplitFunction.class, $("myField")))
                    .select($("myField"), $("word"), $("length"));
            streamTableEnv.from("MyTable")
                    .leftOuterJoinLateral(call(SplitFunction.class, $("myField")))
                    .select($("myField"), $("word"), $("length"));
            streamTableEnv.from("MyTable")
                    .leftOuterJoinLateral(call(SplitFunction.class, $("myField")).as("newWord","newLength"))
                    .select($("myField"), $("newWord"), $("length"));
            streamTableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
            streamTableEnv.from("MyTable")
                    .joinLateral(call("SplitFunction", $("myField")))
                    .select($("myField"), $("word"), $("length"));
            streamTableEnv.sqlQuery("select myField,word,length " +
                    "from MyTable, lateral table(SplitFunction(myField))");
            streamTableEnv.sqlQuery("select myField,word,length " +
                    "from MyTable left join lateral table(SplitFunction(myField)) on true");
            streamTableEnv.sqlQuery("select myField,newWod,newLength " +
                    "from MyTable left join lateral table(SplitFunction(myField)) as T(newWord, newLength) on true");
        }

        /*-=--=-=-=-=-=-=-=- 用户还可以自定义聚合函数 user-defined aggregate function(UDAGG) =-=-=-=-=-=-=-=-=-=-=-*/
        //Accumulator可被flink checkpointing机制自动管理，确保 exactly-once 语义
        public static class WeightedAvgAccumulator{
            public long sum = 0;
            public int count = 0;
        }
        public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccumulator>{
            @Override
            public WeightedAvgAccumulator createAccumulator() {
                return new WeightedAvgAccumulator();
            }
            @Override
            public Long getValue(WeightedAvgAccumulator acc) {
                if (acc.count == 0){
                    return null;
                }else {
                    return acc.sum / acc.count;
                }
            }
            public void accumulate(WeightedAvgAccumulator acc, Long ivalue, Integer iweight){
                acc.sum += ivalue * iweight;
                acc.count += iweight;
            }
            //上述3个方法是强制的，merge方法在window处理下会使用到，retract在over window下会被使用到（都实现掉）
            public void retract(WeightedAvgAccumulator acc, Long ivalue, Integer iweight){
                acc.sum -= ivalue * iweight;
                acc.count -= iweight;
            }
            public void merge(WeightedAvgAccumulator acc, Iterable<WeightedAvgAccumulator> it){
                for (WeightedAvgAccumulator ac : it){
                    acc.count += ac.count;
                    acc.sum += ac.sum;
                }
            }
            public void resetAccumulator(WeightedAvgAccumulator acc){
                acc.count = 0;
                acc.sum = 0L;
            }

            //@Override 见 Top2AccumulatorFunc
            //public Set<FunctionRequirement> getRequirements() {
            //    return null;
            //}
        }
        static void testUDAGG(){
            streamTableEnv.from("MyTable")
                    .groupBy($("myField"))
                    .select($("myField"), call(WeightedAvg.class, $("value"), $("weight")));
            streamTableEnv.createTemporarySystemFunction("WeightedAvg", WeightedAvg.class);
            streamTableEnv.from("MyTable")
                    .groupBy($("myField"))
                    .select($("myField"), call("WeightedAvg", $("value"), $("weight")));
            streamTableEnv.sqlQuery("select myField, WeightedAvg(`value`, weight) from MyTable group by myField");
        }

        /*-==-=-=-=-=-=-=-=--= Table Aggregate Functions =-=-=-=-=-=-=-=-=-=-=-=-*/
        public static class Top2Accumulator{
            public Integer first;
            public Integer second;
        }
        public static class Top2AccumulatorFunc extends TableAggregateFunction<Tuple2<Integer,Integer>, Top2Accumulator>{
            @Override
            public Top2Accumulator createAccumulator() {
                Top2Accumulator top2Acc = new Top2Accumulator();
                top2Acc.first = Integer.MIN_VALUE;
                top2Acc.second = Integer.MIN_VALUE;
                return top2Acc;
            }
            public void accumulate(Top2Accumulator acc, Integer value){
                if (value > acc.first){
                    acc.second = acc.first;
                    acc.first = value;
                }else if (value > acc.second){
                    acc.second = value;
                }
            }
            public void merge(Top2Accumulator acc, Iterable<Top2Accumulator> it) {
                for (Top2Accumulator otherAcc : it) {
                    accumulate(acc, otherAcc.first);
                    accumulate(acc, otherAcc.second);
                }
            }
            public void emitValue(Top2Accumulator acc, Collector<Tuple2<Integer, Integer>> out) {
                // emit the value and rank
                if (acc.first != Integer.MIN_VALUE) {
                    out.collect(Tuple2.of(acc.first, 1));
                }
                if (acc.second != Integer.MIN_VALUE) {
                    out.collect(Tuple2.of(acc.second, 2));
                }
            }
            /*
            emitUpdateWithRetract 可以用于提升性能，在retract模式下emit已更新的数据
            如record更新时引起topN数据变化，此时emitValue需要执行N此，此方法可以增量emit update结果 避免频繁emit数据以提升性能
            */

            //如果希望这个agg函数只在over window上使用，可以声明下面的getRequirements 方法
            @Override
            public Set<FunctionRequirement> getRequirements() {
                Set<FunctionRequirement> set = new HashSet<>();
                set.add(FunctionRequirement.OVER_WINDOW_ONLY);
                return set;
            }
        }
        static void testUDTAGG(){
            streamTableEnv.from("MyTable")
                    .groupBy($("myField"))
                    .flatAggregate(call(Top2AccumulatorFunc.class, $("value")))
                    .select($("myField"), $("f0"), $("f1"));
            streamTableEnv.from("MyTable")
                    .groupBy($("myField"))
                    .flatAggregate(call(Top2AccumulatorFunc.class, $("value")).as("value", "rank"))
                    .select($("myField"), $("value"), $("rank"));
            streamTableEnv.createTemporarySystemFunction("Top2AccumulatorFunc", Top2AccumulatorFunc.class);
            streamTableEnv.from("MyTable")
                    .groupBy($("myField"))
                    .flatAggregate(call("Top2AccumulatorFunc", $("value")).as("value", "rank"))
                    .select($("myField"), $("value"), $("rank"));

        }

        /*-=-=-=-=-=---==-=-=-=- 上述提及的 emitUpdateWithRetract 方法的使用案例 =-=-=-=-=-=----=-=-=-=-=-=-=-*/
        public static class Top2WithRetractAccumulator {
            public Integer first;
            public Integer second;
            public Integer oldFirst;
            public Integer oldSecond;
        }
        public static class Top2WithRetract extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2WithRetractAccumulator> {
            @Override
            public Top2WithRetractAccumulator createAccumulator() {
                Top2WithRetractAccumulator acc = new Top2WithRetractAccumulator();
                acc.first = Integer.MIN_VALUE;
                acc.second = Integer.MIN_VALUE;
                acc.oldFirst = Integer.MIN_VALUE;
                acc.oldSecond = Integer.MIN_VALUE;
                return acc;
            }
            public void accumulate(Top2WithRetractAccumulator acc, Integer v) {
                if (v > acc.first) {
                    acc.second = acc.first;
                    acc.first = v;
                } else if (v > acc.second) {
                    acc.second = v;
                }
            }
            public void emitUpdateWithRetract(Top2WithRetractAccumulator acc,RetractableCollector<Tuple2<Integer, Integer>> out){
                //如果first被更新了才进行emit
                if (!acc.first.equals(acc.oldFirst)){
                    if (acc.oldFirst != Integer.MIN_VALUE){
                        out.retract(Tuple2.of(acc.oldFirst, 1));
                    }
                    out.collect(Tuple2.of(acc.first, 1));
                    acc.oldFirst = acc.first;
                }
                //如果second存在更新才考虑进行collect 即emit 结果
                if (!acc.second.equals(acc.oldSecond)) {
                    if (acc.oldSecond != Integer.MIN_VALUE) {
                        out.retract(Tuple2.of(acc.oldSecond, 2));
                    }
                    out.collect(Tuple2.of(acc.second, 2));
                    acc.oldSecond = acc.second;
                }
            }
        }

        /** -=-=-=--=-=-=-= module 模块操作 -=-=-=-=-=-=-=-=-==-=-**/
        static void testModuleOp(){
            //使用SQL API
            streamTableEnv.executeSql("show modules").print();
            streamTableEnv.executeSql("show full modules").print();
            streamTableEnv.executeSql("load module hive with('hive-version' = '...')");
            streamTableEnv.executeSql("user modules hive,core");
            streamTableEnv.executeSql("unload module hive");

            //使用TableAPI
            streamTableEnv.listModules();
            streamTableEnv.listFullModules();
            //streamTableEnv.loadModule("hive", new HiveModule());
            //修改module的解析顺序
            streamTableEnv.useModules("hive", "core");
            streamTableEnv.unloadModule("hive");
        }
        /** -=-=-=--=-=-=-= catalog 操作 -=-=-=-=-=-=-=-=-==-=-**/
        public static abstract class MyCatalog implements Catalog{
            //要实现的方法极多
        }
        static void testCatalogOp() throws Exception{
            Map<String,String> properties = new HashMap<>();
            CatalogDatabaseImpl catalogDatabase = new CatalogDatabaseImpl(properties, "这是评论");
            MyCatalog myCatalog = null;//new MyCatalog();
            streamTableEnv.registerCatalog("myCatalog", myCatalog);
            streamTableEnv.executeSql("create database/table ....");
            streamTableEnv.listTables();// 打印注册的cataog下的所有表
            streamTableEnv.listCatalogs();

            //catalog api 演示
            myCatalog.createDatabase("mydb", catalogDatabase, false);
            myCatalog.dropDatabase("mydb", false);
            myCatalog.alterDatabase("mydb", catalogDatabase, false);
            myCatalog.getDatabase("mydb");
            myCatalog.databaseExists("mydb");
            myCatalog.listDatabases();

            //catalog操作 Table View Partition Function
            //myCatalog.createTable(new ObjectPath("mydb", "mytable"), new CatalogTableImpl(...), false);
            myCatalog.dropTable(new ObjectPath("mydb", "mytable"), false);
            myCatalog.renameTable(new ObjectPath("mydb", "mytable"), "my_new_table", false);
            CatalogBaseTable table = myCatalog.getTable(new ObjectPath("mydb", "myTable"));
            myCatalog.tableExists(new ObjectPath("mydb", "myTable"));
            myCatalog.listTables("mydb");
            myCatalog.listViews("mydb");

            //myCatalog.createPartition(new ObjectPath("",""), new CatalogPartitionSpec(...), new CatalogParitiionImpl(), false);
            //myCatalog.dropPartition(new ObjectPath("mydb", "mytable"), new CatalogPartitionSpec(...), false);
            myCatalog.listPartitions(new ObjectPath("mydb", "mytable"));
            //myCatalog.listPartitionsByFilter(new ObjectPath("mydb", "mytable"), Arrays.asList(expression));

            myCatalog.dropFunction(new ObjectPath("mydb", "myfunc"), false);
            myCatalog.getFunction(new ObjectPath("mydb", "myfunc"));
            myCatalog.functionExists(new ObjectPath("mydb", "myfunc"));
            myCatalog.listFunctions("mydb");
        }

        static void testTableConfig(){
            // access flink configuration
            Configuration configuration = streamTableEnv.getConfig().getConfiguration();
            // set low-level key-value options
            configuration.setString("table.exec.mini-batch.enabled", "true");
            configuration.setString("table.exec.mini-batch.allow-latency", "5 s");
            configuration.setString("table.exec.mini-batch.size", "5000");

            /** 如何开启 Local-Global Aggregations **/
            configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
            /** 如何开启 Split-Distinct Aggregations **/
            configuration.setString("table.optimizer.distinct-agg.split.enabled", "true");
        }

        public static void main(String[] args) {
            testModuleOp();
        }
    }

    /*-=-=-=-=-=-=-=-=-=-=-= Table API使用案例 =-=-=-=-=-=-=-=-=-=-=-=-*/
    static class GenerateOrderTable{
        @AllArgsConstructor @NoArgsConstructor
        public static class OrderInfo{
            public Long userId;
            public String activityNo;
            public String orderNo;
            public BigDecimal price;
            public Instant createTime;
        }
        @AllArgsConstructor @NoArgsConstructor
        public static class OrderInfoWrapper{
            public OrderInfo f0;
        }
        public static Table createOrderTable(){
            DataStreamSource<OrderInfo> orderInfoStreamSource = streamEnv.fromElements(
                    new OrderInfo(12L, "ac01", "T003", new BigDecimal("12.5"), Instant.parse("2022-03-10T10:15:30.Z")),
                    new OrderInfo(12L, "ac01", "T004", new BigDecimal("120.00"), Instant.parse("2022-03-10T10:16:30.Z")),
                    new OrderInfo(13L, "ac01", "T005", new BigDecimal("12.50"), Instant.parse("2022-03-10T10:16:50.Z")),
                    new OrderInfo(13L, "ac02", "T006", new BigDecimal("24.00"), Instant.parse("2022-03-10T10:17:30.Z")),
                    new OrderInfo(14L, "ac02", "T007", new BigDecimal("65.00"), Instant.parse("2022-03-10T10:19:30.Z")),
                    new OrderInfo(14L, "ac03", "T008", new BigDecimal("53.00"), Instant.parse("2022-03-10T10:20:20.Z")),
                    new OrderInfo(14L, "ac03", "T009", new BigDecimal("105.00"), Instant.parse("2022-03-10T10:20:40.Z")),
                    new OrderInfo(15L, "ac03", "T013", new BigDecimal("150.00"), Instant.parse("2022-03-10T10:22:30.Z")),
                    new OrderInfo(15L, "ac03", "T023", new BigDecimal("180.00"), Instant.parse("2022-03-10T10:23:30.Z"))
            );
            /*
             DataTypes.STRUCTURED 使用时会报错 Unable to find a field named 'f0' in the physical data type derived from the given type information for schema declaration
             DataType orderInfoDataType = DataTypes.STRUCTURED(OrderInfo.class,
                DataTypes.FIELD("userId", DataTypes.BIGINT()),
                DataTypes.FIELD("activityNo", DataTypes.STRING()),
                DataTypes.FIELD("orderNo", DataTypes.STRING()),
                DataTypes.FIELD("price", DataTypes.DECIMAL(22, 6)),
                DataTypes.FIELD("createTime", DataTypes.TIMESTAMP_LTZ(0))
            );
            Schema schema0 = Schema.newBuilder().column("f0", orderInfoDataType).build();
            Table table0 = streamTableEnv.fromDataStream(orderInfoStreamSource, schema0).as("OrderInfo");
            */
            Schema schema = Schema.newBuilder()
                    .column("userId", DataTypes.BIGINT())
                    .column("activityNo", DataTypes.STRING())
                    .column("orderNo", DataTypes.STRING())
                    .column("price", DataTypes.DECIMAL(22, 6))
                    .column("createTime", DataTypes.TIMESTAMP_LTZ(0))
                    //不指定 rowtime / proctime 字段，使用窗口函数会报 window expects a time attribute for grouping in a stream environment
                    .columnByExpression("rowtime","cast(createTime as timestamp_ltz(3))")
                    .watermark("rowtime", "rowtime - INTERVAL '5' SECOND")
                    .build();
            Table table = streamTableEnv.fromDataStream(orderInfoStreamSource, schema);
            return table;
        }
        //Table API 一个简单的group by 查询
        static void test1(Table orderInfoTable){
            Table groupByUserIdSumPriceTable = orderInfoTable.groupBy($("userId")).select($("userId"), $("price").sum().as("totalPay"));
            groupByUserIdSumPriceTable.execute().print();
        }
        //Table API复杂案例： filter过滤字段为空的记录 +
        static void test2(Table orderInfoTable){
            Table resultTable = orderInfoTable.filter(
                    //and来自 org.apache.flink.table.api.Expressions 即 Expression DSL
                    and($("userId").isNotNull(), $("activityNo").isNotNull(), $("orderNo").isNotNull())
            )
                    .select($("userId").as("user_id"), $("orderNo").lowerCase().as("order_no"), $("price"), $("createTime"), $("rowtime"))
                    //Tumble.over(lit(90).seconds()) 相当于 Tumble.over("90.seconds")
                    .window(Tumble.over(lit(90).seconds()).on($("rowtime")).as("widow_name_n"))
                    //大概 table api中groupby总是两个字段，第一个总是窗口，第二个是java api中的 keyBy 使用的字段
                    .groupBy($("widow_name_n"), $("user_id"))
                    .select($("user_id"),
                            $("widow_name_n").start().as("window_start"),
                            $("widow_name_n").end().as("window_end"),
                            $("price").avg().as("avg_window_price"),
                            $("order_no").count().as("window_order_count"));
                    //.orderBy($("user_id").asc()); -- 如何排序？？？
            resultTable.execute().print();
        }

        public static void main(String[] args) {
            Table orderInfoTable = createOrderTable();
            orderInfoTable.printSchema();
            test2(orderInfoTable);
        }
    }


















}