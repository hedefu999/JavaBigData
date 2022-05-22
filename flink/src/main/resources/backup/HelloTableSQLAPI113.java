package com.bigdata.flink.primary.v13;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TumbleWithSizeOnTimeWithAlias;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/*
依赖 flink-table_2.11 1.7.0+ 的Table API写法
flink版本1.7.+ 《Flink原理、实战与性能优化》 - 张利兵
不推荐

luweizheng 《Flink原理与实践》 使用版本 1.11.0

本项目使用 1.13.5 采用 Flink原理与实践 中的API
 */
public class HelloTableSQLAPI113 {
    static StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    static ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
    static final StreamTableEnvironment tableStreamEnv = StreamTableEnvironment.create(streamEnv);
    static final BatchTableEnvironment tableBatchEnv = BatchTableEnvironment.create(batchEnv);

    /*
	关系型编程接口 Table API 及 SQL API
	 */
    public static void main(String[] args) {

    }
    /*
    flink cataLog进行元数据的管理
    注册相应的数据源和数据表信息，所有对数据库和表的元数据信息存放在FLink CataLog 内部目录结构中，
    存放了Flink内部所有与Table相关的元数据信息，包括表结构信息、数据源信息
    注册在CataLog中的Table类似关系型数据库的视图结构，当注册的表被引用和查询时数据才会在对应的Table中生成
    需要注意：多个语句同时查询一张表时，表中的数据会被执行多次，且每次查询出来的结果相互之间不共享
     */
    static class AboutCataLogRegistration{
        /*
        通过内部CataLog注册一些信息
         */
        static void registerTableByInnerCataLog(){
            Table table = tableStreamEnv.scan("SourceTable").select("null");
            //projectedTable 注册在CataLog中的表名，第二个参数是table对象
            tableStreamEnv.registerTable("projectedTable", table);

            //使用csv文件注册一个Table Source
            String[] columns = {"name","age","time"};
            TypeInformation<Object>[] types = new TypeInformation[]{TypeInformation.of(String.class), TypeInformation.of(Integer.class), TypeInformation.of(Long.class)};
            CsvTableSource csvTableSource = new CsvTableSource("/path/to/file", columns, types);
            tableStreamEnv.registerTableSource("myFirstTableSource", csvTableSource);

            //TableSink的注册：将结果写入Csv文件
            CsvTableSink csvTableSink = new CsvTableSink("path/csvFile", ",");
            tableStreamEnv.registerTableSink("myfisrtTableSink",columns,types, csvTableSink);
        }
        /*
        通过外部CataLog注册信息
        除了使用Flink内部的CataLog作为所有Table数据的元数据的存储介质外，也可以使用外部CataLog
        Table/SQL API可以将临时表注册在外部CataLog中
         */
        static void registerTableSourceByExternalCataLog(){
            //注册一个基于内存的外部CataLog
            InMemoryExternalCatalog externalCatalog = new InMemoryExternalCatalog("myfirstInMemoryExternalCatalog");
            tableStreamEnv.registerExternalCatalog("registeredInMemCataLog", externalCatalog);
        }
        /*
        Table API是构建在DataStream API和DataSet API上的一层更高级的抽象，因此可以灵活地使用Table API将Table转成DataStream或DataSet数据集
        也可以将DataStream或DataSet数据集转换成Table，这类似Spark中的DataFrame和RDD
         */
        static void convertBetweenDataStreamSetAndTable(){
            DataStreamSource<Tuple3<Long, String, Integer>> streamSource = streamEnv.fromElements(Tuple3.of(12L, "name", 13));
            DataSource<Tuple3<Long, String, Integer>> setSource = batchEnv.fromElements(Tuple3.of(12L, "name", 13));

            //## DataStream/ DataSet --注册成--> Table
            //将DataStream注册成Table，指定表名并使用DataStream中的全部字段作为表字段
            //注册的表名必须在应用内唯一
            tableStreamEnv.registerDataStream("unique_table_1", streamSource);//register方法没有返回
            //如果只需要部分字段作为表字段，可以通过一个String传递
            tableStreamEnv.registerDataStream("unique_table_2", streamSource, "f0,f1");

            //## DataStream / DataSet --转换成--> Table
            Table table1 = tableStreamEnv.fromDataStream(streamSource);
            Table table2 = tableStreamEnv.fromDataStream(streamSource, "f0,f2");//只要部分字段

            //## DataSet 注册成 Table（表名必须唯一,无返回）
            tableBatchEnv.registerDataSet("tableName", setSource);
            tableBatchEnv.registerDataSet("tableName", setSource, "f0,f2");

            //## DataSet 转换成 Table
            Table table3 = tableBatchEnv.fromDataSet(setSource);
            Table table4 = tableBatchEnv.fromDataSet(setSource, "f0,f1");

            //## Table 转换成 DataStream
            DataStream<Tuple3<Long, String, Integer>> dataStream = tableStreamEnv.toAppendStream(table1,
                    TypeInformation.of(new TypeHint<Tuple3<Long, String, Integer>>() {
                    }));
            // ### 直接转成Row格式可以临时回避繁琐的类型信息
            DataStream<Row> rowDataStream = tableStreamEnv.toAppendStream(table1, Row.class);
            // 使用retract模式转换table,第一个字段必须是Boolean
            StreamQueryConfig streamQueryConfig = new StreamQueryConfig();
            streamQueryConfig.withIdleStateRetentionTime(null, null);
            //返回的Boolean字段表示数据更新类型，true表示insert操作更新的数据，false表示delete操作更新的数据
            DataStream<Tuple2<Boolean, Tuple3<Boolean, Long, String>>> retractStream1 = tableStreamEnv.toRetractStream(table1,
                    TypeInformation.of(new TypeHint<Tuple3<Boolean, Long, String>>() {
                    }), streamQueryConfig);

            //Table 转换成 DataSet
            DataSet<Row> dataSet = tableBatchEnv.toDataSet(table3, Row.class);
        }
        /*
        schema 字段映射
        Position-Based 字段位置映射
         */
        static void schemaFieldsMapping(){
            String fields = "f1,f2";
            fields = "_1,_2";
            fields = "f0 as id,f1 as name, f2 as age"; //pojo 类型与此类似
            Table table = tableStreamEnv.fromDataStream(streamEnv.fromElements(Tuple3.of(12L, "name", 13)), fields);
        }
    }
    //# 外部连接器
		/*
		Table Connector
		Flink中内置的Table Connector有 File System Connector、Kafka Connector、Elasticsearch Connector
		 */
    static class TableConnector{
        /*
        各种类型的Connector
         */
        public static class CustomFlinkKafkaPartitioner extends FlinkKafkaPartitioner<Tuple3<Long,String,Integer>> {
            @Override
            public int partition(Tuple3<Long, String, Integer> record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
                //一个记录写入到kafka哪个分区上在这里指定
                return 0;
            }
        }
        static void tableConnector(){
            //FileConnector
            tableStreamEnv.connect(new org.apache.flink.table.descriptors.FileSystem().path("/path/fileName"));
            //KafkaConnector
            //需要添加 flink-connector-kafka 依赖，官方资料 https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/kafka/
            //资料里提到一个KafkaSource的API   KafkaSource.builder().setTopics("topic").build()
            Kafka kafka = new Kafka().version("0.10")
                    .topic("topic")
                    .property("zookeeper.connect", "127.0.0.1:2181")
                    .property("bootstrap.servers", "127.0.0.1:9092")
                    .property("group.id", "group_id")
                    //指定启动时Kafka的消费模式：offset如何确定
                    .startFromEarliest() // 从最早的offset开始消费
                    .startFromLatest() //从最新的offset开始消费
                    .startFromSpecificOffset(3, new Date().getTime()) //从指定的offset开始消费
                    //向Kafka中写入数据时 指定Flink和Kafka的数据分区策略
                    .sinkPartitionerFixed() //每个Flink分区最多被分配到一个Kafka分区上
                    .sinkPartitionerRoundRobin() //Flink中分区随机映射到Kafka分区上
                    .sinkPartitionerCustom(CustomFlinkKafkaPartitioner.class);//使用class传递自定义分区规则
            tableStreamEnv.connect(kafka);
        }
        static void tableFormat(){
            StreamTableDescriptor streamTableDescriptor = tableStreamEnv.connect(new Kafka().topic(""));
            //对于Stream类型的Table数据，需要标记出是 INSERT UPDATE DELETE 中的哪种操作更新的数据
            streamTableDescriptor.inAppendMode(); //仅交互 INSERT 操作更新的数据
            streamTableDescriptor.inUpsertMode(); //仅交互 INSERT UPDATE DELETE 操作更新的数据
            streamTableDescriptor.inRetractMode();//仅交互 INSERT DELETE 操作更新的数据
            StreamTableDescriptor streamTableDescriptor1 = streamTableDescriptor.inAppendMode();
            StreamTableDescriptor streamTableDescriptorLast = streamTableDescriptor1.withFormat(
                    new Csv()
                            .field("id", org.apache.flink.api.common.typeinfo.Types.LONG)
                            .field("name", org.apache.flink.api.common.typeinfo.Types.STRING)
                            .fieldDelimiter(",")
                            .lineDelimiter("\n")
                            .quoteCharacter('*')
                            .commentPrefix("#")
                            .ignoreFirstLine() //是否忽略 headLine
                            .ignoreParseErrors() //是否忽略解析错误
            ).withSchema(//使用Table Schema 定义字段，flink json format 的schema方法已废弃，使用这里的API
                    new Schema()
                            .field("id_alias", TypeInformation.of(Long.class))
                            //Table Schema中字段出现的顺序必须与Input/Output数据源中的字段顺序一致
                            .field("name_alias", TypeInformation.of(String.class))
            );
				/*
				三种方式定义JSON Format, JSON Format从table schema 中产生
				Json的包名：org.apache.flink.table.descriptors
				 */
            streamTableDescriptor1.withFormat(
                    new Json()
                            .failOnMissingField(false)
                            .schema(null)
                            .jsonSchema("") //这个字符串相当恶心，是该弃用
                            .deriveSchema()
                            .ignoreParseErrors(false)
            );
            //Table Schema支持复杂的字段映射和特殊的时间获取
            streamTableDescriptor1.withSchema(
                    new Schema()
                            .field("Field1", org.apache.flink.api.common.typeinfo.Types.SQL_TIMESTAMP)
                            .proctime() //获取Process Time
                            .field("Field2", org.apache.flink.api.common.typeinfo.Types.SQL_TIMESTAMP)
                            .rowtime(null) //获取Event Time
                            .field("Field3", org.apache.flink.api.common.typeinfo.Types.BOOLEAN)
                            .from("origin_field_name") //从Input/Output数据指定字段中获取数据
            );
            //rowtime获取EventTime需要这样写：
            streamTableDescriptor1.withSchema(
                    new Schema()
                            .field("", org.apache.flink.api.common.typeinfo.Types.SQL_TIMESTAMP)
                            .rowtime( //如何获取Event Time
                                    new Rowtime()
                                            .timestampsFromField("page_start_time") //根据字段名称从输入数据中提取
                                            .timestampsFromSource() //从底层DataStream API中转换而来，数据源需要支持分配时间戳（Kafka 0.10+）
                                            .timestampsFromExtractor(null) //需要实现一个 TimestampExtractor
                            ).rowtime( //指定Watermark策略
                            new Rowtime() //下面3个方法选一个
                                    .watermarksPeriodicBounded(5000) //设置Watermark
                                    .watermarksPeriodicAscending() //和rowtime最大时间保持一致
                                    .watermarksFromSource() //使用底层DataStream API内建的Watermark
                    )
            );


        }

        /*
        # 实用案例：Kafka Table Connector
         */
        public static void main(String[] args) {
            tableStreamEnv.connect(
                    new Kafka()
                            .version("0.10") //kafka消息版本
                            .topic("test-topic")
                            .startFromEarliest()
                            .property("zookeeper.connect", "localhost:2181")
                            .property("bootstrap.servers", "localhost:9092")
            ).withFormat(
                    new Json()
                            .failOnMissingField(true)
                            .ignoreParseErrors(true)
            ).withSchema(
                    new Schema()
                            .field("id", org.apache.flink.api.common.typeinfo.Types.LONG)
                            .field("name", org.apache.flink.api.common.typeinfo.Types.STRING)
            ).inAppendMode().registerTableSource("KafkaInputTable");
        }
    }

    static class TableTimeConcepts{
        static void test(){
            DataStreamSource<Tuple3<Long, String, Long>> inputStream = streamEnv.fromElements(Tuple3.of(16345385793L, "query_goods", 12343L));
            //在DataStream转换Table过程中定义Event Time字段
            WatermarkStrategy<Tuple3<Long, String, Long>> watermarkStrategy = WatermarkStrategy.<Tuple3<Long, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(6))
                    .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<Long, String, Long>>() {
                        @Override
                        public long extractTimestamp(Tuple3<Long, String, Long> element, long recordTimestamp) {
                            return element.f0;
                        }
                    });
            SingleOutputStreamOperator<Tuple3<Long, String, Long>> watermarkStream =
                    inputStream.assignTimestampsAndWatermarks(watermarkStrategy);
            Table table = tableStreamEnv.fromDataStream(watermarkStream, "f0,f1,f2");
            //table.window(null);
        }
    }
    //一个相对完整的stream转table示例
    static class TableSource{
        static class MyStreamTableSource implements DefinedRowtimeAttributes, StreamTableSource<Row> {
            @Override //定义Table API中的时间属性信息
            public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
                //创建基于event_time的RowtimeAttributeDescriptor,确定时间属性信息
                RowtimeAttributeDescriptor descriptor = new RowtimeAttributeDescriptor(
                        "event_time", //时间属性名称
                        new ExistingField("event_time"),
                        new AscendingTimestamps()
                );
                return Collections.singletonList(descriptor);
            }
            @Override //StreamTableSource#getDataStream() 定义输入数据源
            public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
                //定义获取DataStream数据集的逻辑
                DataStreamSource<Tuple3<String, String, Long>> dataStreamSource =
                        streamEnv.fromElements(Tuple3.of("Id234", "Val586", 16489734834L));
                WatermarkStrategy<Tuple3<String, String, Long>> watermarkStrategy =
                        WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>)
                                        (element, recordTimestamp) -> element.f2);
                SingleOutputStreamOperator<Row> singleOutputStreamOperator =
                        dataStreamSource.assignTimestampsAndWatermarks(watermarkStrategy).map(
                                (MapFunction<Tuple3<String, String, Long>, Row>) value ->
                                        Row.of(value.f0, value.f1, value.f2));
                return singleOutputStreamOperator;
            }
            @Override //定义数据集字段名称和类型
            public TypeInformation<Row> getReturnType() {
                String[] names = {"id","value","event_time"};
                TypeInformation[] types = {org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.LONG};
                return org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(names, types);
            }
            @Override
            public TableSchema getTableSchema() {
                return TableSchema.builder()
                        .field("id", org.apache.flink.api.common.typeinfo.Types.STRING)
                        .field("value", TypeInformation.of(String.class))
                        .field("event_time", BasicTypeInfo.LONG_TYPE_INFO).build();
            }
            @Override
            public String explainSource() {
                return "tell me how to explain source!";
            }
        }

        public static void main(String[] args) {
            String tableName = "my_stream_table";
            //注册输入数据源到tableEnv
            tableStreamEnv.registerTableSource(tableName, new MyStreamTableSource());
            //在窗口中处理输入数据 ??? todo 很奇怪的写法
            TumbleWithSizeOnTimeWithAlias window =
                    Tumble.over("10.minutes").on("event_time").as("my_table_tumble_window");
            tableStreamEnv.scan(tableName).window(window);
        }
    }
    //ProcessTime指定(两种方式)
    static class ProcessTimeAssign{
        //通过TableSource函数定义ProcessTime
        static class MyProcessTimeInputEventSource implements DefinedProctimeAttribute,StreamTableSource<Row>{
            @Nullable
            @Override //定义Table API中的时间属性信息，该字段会被添加到Schema尾部
            public String getProctimeAttribute() {
                return "process_time";
            }
            @Override
            public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
                //定义获取DataStream数据集的逻辑
                DataStreamSource<Row> rows = execEnv.fromElements(Row.of(234539L, "jack", 16232890400L));
                //不需要指定Watermark信息
                return rows;
            }
            @Override
            public TypeInformation<Row> getReturnType() {
                return null;
            }
            @Override
            public TableSchema getTableSchema() {
                return null;
            }
        }
        public static void main(String[] args) {
            //1.在DataStream转换Table的过程中定义process_time
            DataStreamSource<Tuple4<Long, String, Integer,Long>> streamSource =
                    streamEnv.fromElements(Tuple4.of(1245L, "jack", 12, 16234893634L));
            //.proctime 专门用于指定哪个字段是process_time
            Table table = tableStreamEnv.fromDataStream(streamSource, "id,name,age,birth.proctime");
            //基于process_time时间属性创建翻滚窗口
            WindowedTable window = table.window(Tumble.over("10.minutes").on("process_time").as("window"));

            //2.通过TableSource函数定义
            tableStreamEnv.registerTableSource("ProcessTimeInputEventSource", new MyProcessTimeInputEventSource());
            tableStreamEnv.scan("ProcessTimeInputEventSource")
                    .window(Tumble.over("10.minutes").on("process_time").as("window"));
        }
    }

    //临时表
    static class TemporalTables{
        public static void main(String[] args) {
            DataStreamSource<Tuple3<Long, String, Long>> streamSrc =
                    streamEnv.fromElements(Tuple3.of(325346L, "jack", 16346823434L));
            //书上使用scala api写的，java用不了toTable方法
            //DataStreamConversions dataStreamConversions =
            //		new DataStreamConversions(streamSrc, TypeInformation.of(new TypeHint<Tuple2<Long, Integer>>() {
            //}));
            //Table table = dataStreamConversions.toTable(tableStreamEnv, "f0,f1");
            Table table = tableStreamEnv.fromDataStream(streamSrc, "f0,f1,f2.proctime");//.rowtime
            //在Table API中直接调用 Temporal Table Function,指定 时间字段 和 主键
            TemporalTableFunction tableFunction = table.createTemporalTableFunction("f2", "f0");
            //在TableEnvironment中注册 tableFunction信息,然后在SQL中通过名称调用
            tableStreamEnv.registerFunction("myFirstTemporalTable", tableFunction);
        }
    }

    /*
    # Flink Table API 介绍
    上面介绍了TableAPI外围内容，进行知识铺垫
     */
    static class RealFlinkTableAPI{
        public static void main(String[] args) {
            //registerTable
            //通过scan方法在CataLog中找到表： sensors_sample
            Table sensorsTable = tableStreamEnv.scan("sensors_sample");
            Table resultTable = sensorsTable.groupBy("id")
                    .select("id,val1.sum as val1Sum")
                    .as("id,val1Sum");

            //## 数据查询和过滤
            sensorsTable.select("id,val1 as myval1");
            sensorsTable.select("*");
            //按照字段位置进行重命名
            sensorsTable.as("f0,f1,f2,f3");
            //使用filter进行数据筛选
            sensorsTable.filter("val1%2 = 0");//字符型字段 "val1 = 'saof'"
            //使用where方法进行数据筛选
            sensorsTable.where("id = '1000134'");

				/*
				## 窗口操作
				在Table API中使用window方法对窗口进行定义和调用，必须通过as方法指定别名供后面的算子使用
				 */
            sensorsTable.window(
                    Session.withGap("90.second").on("f0").as("sessionWindow")
            )//指定窗口类型并重命名
                    .groupBy("sessionWindow") //对窗口进行聚合，窗口数据会分配到单个Task算子中
                    .select("val1.sum");//聚合函数还支持min max
        }
    }

}
