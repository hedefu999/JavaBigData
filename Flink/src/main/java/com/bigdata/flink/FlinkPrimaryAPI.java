package com.bigdata.flink;

import com.bigdata.flink.pojos.PageEvent;
import com.bigdata.flink.pojos.Person;
import com.bigdata.flink.pojos.ViewEvent;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.operators.join.JoinFunctionAssigner;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.DeltaEvictor;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.InMemoryExternalCatalog;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FlinkPrimaryAPI {
	static Logger logger = LoggerFactory.getLogger("StreamingJob");
	static StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
	static ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
	static final StreamTableEnvironment tableStreamEnv = TableEnvironment.getTableEnvironment(streamEnv);
	static final BatchTableEnvironment tableBatchEnv = TableEnvironment.getTableEnvironment(batchEnv);

	static class FlinkProgrammeStructure{
		static ExecutionEnvironment dataSetEnv = ExecutionEnvironment.getExecutionEnvironment();
		void getEnvironment(){
			//设置运行环境，如果是本地启动就创建本地环境，如果是集群上启动就创建集群环境
			final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			//指定并行度创建本地环境
			LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment(3);
			//指定远程JobManagerIP和RPC端口以及运行程序所在的jar包及依赖包
			StreamExecutionEnvironment env2 = StreamExecutionEnvironment.createRemoteEnvironment(
					"JobManagerHost", 6021, 5, "/user/application.jar");
		}
		/**
		 自定义一个MapFunction，更复杂的可以使用RichMapFunction
		 */
		private void mapFunctionDemo(){
			DataStreamSource<String> strDataSrc = streamEnv.fromElements("hello", "flink");
			strDataSrc.map(new MapFunction<String, String>() {
				@Override
				public String map(String s) throws Exception {
					return s.toUpperCase();
				}
			});
		}
		/**
		 分区key的指定，join\coGroup\groupBy算子需要根据指定的key进行转换
		 */
		//使用Tuple实现上述功能
		static void partitionKeyUsageWithStreamTuple() throws Exception{
			Tuple2<String, Integer> a = new Tuple2<>("a", 1);
			Tuple2<String, Integer> b = new Tuple2<>("b",2);
			Tuple2<String, Integer> c = new Tuple2<>("b",2);
			DataStreamSource<Tuple2<String,Integer>> dataStream = streamEnv.fromElements(a, b, c);
			SingleOutputStreamOperator<Tuple2<String, Integer>> operator = dataStream.keyBy(
					new KeySelector<Tuple2<String, Integer>, String>() {
						@Override
						public String getKey(Tuple2<String, Integer> value) throws Exception {
							return value.f0;
						}
					}).sum("f1");//写数字1也可以
			//dataStream.keyBy("_1").sum("f1"); 这种写法可读性差，已废弃
			operator.print("SinkId_20220108");
		}
		//使用java Pojo 实现一个单词计数功能
		//由于内部类测试的关系，WordWithCount类一旦不声明为public，flink会认为这不是一个pojo类！
		@NoArgsConstructor @AllArgsConstructor @Data
		public static class WordWithCount{
			private String word;
			private Integer count;
			//检查一个java class是不是 pojo：是否返回 PojoSerializer 字样（返回KryoSerializer 就说明不是pojo）
			//这个办法从这个人那里看到的：https://lulaoshi.info/flink/chapter-datastream-api/data-types
			public static void amIPojo(){
				System.out.println(TypeInformation.of(WordWithCount.class).createSerializer(new ExecutionConfig()));
			}
		}
		static void partitionKeyUsageWithPojoTuple(){
			String man = "to be or not to be, that's a question";
			String wan = "oh my god! buy it! buy it! that's fantastic";
			String ani = "good good doggy happy happy ending";
			DataStreamSource<String> source = streamEnv.fromElements(man, wan);
			DataStreamSource<String> source1 = streamEnv.fromElements(ani);
			//union多个数据源，union支持同时传入多个来源 ？？？是否需要使用union返回的source，还是继续使用source？？？
			source.union(source1);
			SingleOutputStreamOperator<WordWithCount> operator =
					source.flatMap(new FlatMapFunction<String, WordWithCount>() {
						@Override
						public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
							String[] s = value.split(" ");
							for (String word : s) {
								out.collect(new WordWithCount(word, 1));
							}
						}
					}).filter(new FilterFunction<WordWithCount>() {
						@Override
						public boolean filter(WordWithCount value) throws Exception {
							return value != null;// && value.word.contains("!");
						}
					});
			//对数据进行重分区的要求：pojo必须提供覆写的hashCode方法；不能是数组结构；
			KeyedStream<WordWithCount, String> keyedStream = operator.keyBy(
					(KeySelector<WordWithCount, String>) pojo -> pojo.word
			);
			//分组后进行组内统计
			keyedStream.reduce(new ReduceFunction<WordWithCount>() {
				@Override
				public WordWithCount reduce(WordWithCount value1, WordWithCount value2) throws Exception {
					return new WordWithCount(value1.word, value1.count + value2.count);
				}
			});
			//keyedStream.sum("count"); 上面的可以简写成sum
			/*
			这种类似java stream api的Collectors中的封装被称为aggregations
			类似的操作还有 min minBy max maxBy
			 */
			operator.print("sink_no_6483");
		}
		//使用DataSet API实现
		static void partitionKeyUsageInDataSetAPI(){
			//operator.addSink(new SinkFunction<Property>() {
			//	@Override
			//	public void invoke(Property value, Context context) throws Exception {
			//		context.currentWatermark();
			//		context.timestamp();
			//
			//	}
			//});
			//DataSource<Property> dataSet = dataSetEnv.fromElements(a, b, c);
			////相同name下的最大age？？？
			//AggregateOperator<Property> aggrOpt = dataSet.groupBy("name").max(1);
		}

		public static void main(String[] args) throws Exception {
			//partitionKeyUsageWithStreamTuple();
			partitionKeyUsageWithPojoTuple();
			//streamEnv需要显示调用execute，DataSetEnv则不需要
			streamEnv.execute("jobNameHello");
		}
	}

	static class FlinkDataTypeInfo{
		void createAllTypeData(){
			DataStreamSource<Integer> intDataSrc = streamEnv.fromElements(1, 2, 3);
			DataStreamSource<String> strDataSrc = streamEnv.fromElements("hello", "flink");
			String[] array = {"a","b","c"};
			//streamEnv.fromCollection(array); java 不支持
			List<String> list = Arrays.asList("a","b","c");
			streamEnv.fromCollection(list);
		}
		//Tuple不支持空值存储，字段数量上限25，超过25需要继承Tuple进行扩展
		void abountTuple(){
			//使用数组？参考Tuple2 ... Tuple4实现一个
			new Tuple() {
				Object[] data = new Object[26];
				@Override
				public <T> T getField(int pos) {
					return null;
				}
				@Override
				public <T> void setField(T value, int pos) {

				}
				@Override
				public int getArity() {
					return 0;
				}
				@Override
				public <T extends Tuple> T copy() {
					return null;
				}
			};
		}

		//TypeHint的使用场景
		void abountTypeHint(){
			streamEnv.fromElements("a","b").flatMap(new FlatMapFunction<String, Integer>(){
				@Override
				public void flatMap(String value, Collector<Integer> out) throws Exception {
					//...
				}
			}).returns(new TypeHint<Integer>() {});//通过returns方法指定返回类型参数
		}
		/*
		 自定义TypeInformation：TypeInfoFactory的使用
		  先通过注解@TypeInfo创建数据类型
		 */
		@TypeInfo(CustomTypeInfoFactory.class)
		public class CustomTuple<T0,T1>{
			public T0 field0;
			public T1 field1;
		}
		//定义TypeInfoFactory实现类
		public class CustomTypeInfoFactory extends TypeInfoFactory<CustomTuple>{
			@Override
			public TypeInformation<CustomTuple> createTypeInfo(Type t,
															   Map<String, TypeInformation<?>> genericParams) {
				//return new CustomTupleTypeInfo(genericParams.get("T0"),genericParams.get("T1"));
				return null;//？？？！书跟网上都这么写，，，
			}
		}
	}

	/**
	 * # DataStream API
	 */
	static class DataStreamAPI{
		//## 内置数据源
		static class InnerDataSource{
			//### 文本数据源(待调试)
			static void fileBasedInput(){
				//1. 简单地从纯文本文件读取
				//DataStreamSource<String> source = streamEnv.readTextFile("");
				//2. 从csv文件读取内容
				String csvFilePath = "/Users/hedefu/Documents/Developer/IDEA/JavaStack/BigData/src/main/resources/file/testcsv.csv";
				Path path = new Path(csvFilePath);
				CsvInputFormat<String> csvInputFormat = new CsvInputFormat<String>(path) {
					@Override
					protected String fillRecord(String s, Object[] objects) {
						logger.info("fileRecord: s = {}, objects = {}",s,objects.length);
						return null;
					}
				};//CsvInputFormat 使用 readFile 写法报错
				//DataStreamSource<String> source1 = streamEnv.readFile(csvInputFormat, csvFilePath);
				DataStreamSource<String> csvSource = streamEnv.readFile(csvInputFormat, csvFilePath,
						FileProcessingMode.PROCESS_ONCE,1000/*ms*/);
				csvSource.setParallelism(1).print("csv_sink_no_8362");
			}
			//### socket 数据源,发送端如何设计
			public void socketDataSource(){
				//从Socket获取数据 ？？？如何演示
				DataStreamSource<String> socketSource = streamEnv.socketTextStream("localhost", 9999);
				socketSource.print("sink_no_2487");
			}

			public static void main(String[] args) throws Exception {
				fileBasedInput();
				streamEnv.execute("csv_job_3829");
			}
		}
		//如何本地向socket端口发送数据被Flink接收到
		static class SocketSinkClientDemo{
			public static void main (String[] args) throws Exception{
				//提供一个序列化器定义
				SocketClientSink<String> clientSink = new SocketClientSink<>("localhost", 9086, new SerializationSchema<String>() {
					@Override
					public byte[] serialize(String element) {
						return element.getBytes();
					}
				}, 0);
				clientSink.open(new Configuration());
				clientSink.invoke("测试 test");
				clientSink.close();
			}
		}


		//## 外部数据源
		static class OutterDataSource{
			public static class OutterData{
				public static OutterData parse(byte[] input){
					return new OutterData();
				}
			}
			//外部数据源有时候需要提供 反序列化器定义  可以参考 SimpleStringSchema 实现
			public class SourceEventSchema implements DeserializationSchema<OutterData>{
				@Override
				public OutterData deserialize(byte[] message) throws IOException {
					return OutterData.parse(message);
				}
				@Override
				public boolean isEndOfStream(OutterData nextElement) {
					return false;
				}
				@Override
				public TypeInformation<OutterData> getProducedType() {
					return TypeInformation.of(OutterData.class);
				}
			}
		}

		//## datasource的 connect coMap coFlatMap操作
		/*
		与union要求必须是数据类型完全一致的union不同，connect允许结构不同的数据集混合
		得到的ConnectedStreams类型不能直接进行类似print操作，需要再转换成DataStream类型数据集
		 */
		static class AbountStreamConnect{
			DataStreamSource<String> strDataSource = streamEnv.fromElements("a", "b");
			DataStreamSource<Person> personDataSource = streamEnv.fromElements(new Person("jack", 12), new Person("lucy", 13));
			void show(){
				ConnectedStreams<String, Person> connect = strDataSource.connect(personDataSource);
				connect.map(new CoMapFunction<String, Person, String>() {
					@Override
					public String map1(String value) throws Exception {
						return value;
					}

					@Override
					public String map2(Person value) throws Exception {
						return "person:"+value.getName();
					}
				});
				//connect.flatMap(...)  flatMap同样需要一个CoMapFunction定义
				/*
				在并行度>1的情况下，CoMapFunction中的两个map方法的执行先后顺序无法保证交替，数据顺序不能保证
				 */
			}
			/*
			借助keyBy或broadcast广播变量实现更复杂的关联效果
			 */
			void showKeyByAndBroadcast(){
				//只使用name字段与字符串datasource连接
				ConnectedStreams<Person, String> personNameStringConnectedStreams =
						personDataSource.connect(strDataSource).keyBy("name", "default");
				//？？？广播的具体用法
				ConnectedStreams<String, Person> connect = strDataSource.connect(personDataSource.broadcast());
				ConnectedStreams<Person, String> connect1 = personDataSource.connect(strDataSource.broadcast());
			}
			/*
			split一个数据集，实际上只是对输入数据进行了标记。真正将数据进行切分需要使用select API
			split已在Flink 1.6 中废弃，不建议使用
			 */
			void showSplitAPI(){
				//personDataSource.setParallelism(2).s ？？？未找到有关split的API
			}
		}
		/*
		数据的物理分区
		 */
		static class AboutPhysicalPartition{
			void show(){
				DataStreamSource<String> source = streamEnv.fromElements("a");
				source.shuffle();//随机分区 Random Partitioning
				source.rebalance();//Roundrobin Partitioning round-robin分区策略，有效避免数据倾斜
				source.rescale();//Rescaling Partitioning 仅对上下游继承的算子数据进行重平衡，具体分区依据上下游算子的并行度确定
				source.broadcast();//Broadcasting 适合大数据集关联小数据集（配置、枚举信息），通过广播将小数据集分发到算子的每个分区中
				//自定义一个分区规则，数据元素符合某些条件就给个分区号，其他不好确定的按随机数指定
				source.partitionCustom(new Partitioner<String>() {
					@Override
					public int partition(String key, int numPartitions) {
						int partition = key.contains("flink") ? 0 : new Random(numPartitions).nextInt();
						return partition;
					}
				}, new KeySelector<String, String>() {
					@Override
					public String getKey(String value) throws Exception {
						return value;
					}
				});//keySelect直接填 0 或 default即可
			}
		}
		/*
		DataSink数据输出
		 */
		static class AboutDataSink{
			static DataStreamSource<String> source = streamEnv.fromElements("hello", "datasink");
			static void show(){
				source.writeToSocket("localhost",9095,new SimpleStringSchema());
			}

			public static void main(String[] args) throws Exception{
				show();
				streamEnv.execute("data_sink_no_001");
			}
		}

		/*
		定时器功能
		在onTimer中执行定时任务内容，并注册下一次触发时间，周而复始
		onTimer会被flink回调
		 */
		static class AboutTimer{
			public static void main(String[] args) {
				DataStreamSource<Tuple3<Long, String, Integer>> source = streamEnv.fromElements(Tuple3.of(12L, "jack", 24));
				KeyedStream<Tuple3<Long, String, Integer>, Long> keyedStream = source.keyBy(item -> item.f0);
				keyedStream.process(new KeyedProcessFunction<Long, Tuple3<Long, String, Integer>, Tuple3<Long, String, Integer>>() {
					boolean first = true;
					@Override
					public void processElement(Tuple3<Long, String, Integer> value, Context ctx, Collector<Tuple3<Long, String, Integer>> out) throws Exception {
						if (first){
							long nextTime = Instant.ofEpochMilli(ctx.timestamp()).atZone(ZoneId.systemDefault()).withHour(0).plusDays(1).toInstant().toEpochMilli();
							ctx.timerService().registerEventTimeTimer(nextTime);
						}
						//process
						out.collect(value);
					}
					@Override
					public void open(Configuration parameters) throws Exception {
						//初始化
					}
					@Override
					public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<Long, String, Integer>> out) throws Exception {
						//执行定时器任务
						long nextTime = Instant.ofEpochMilli(ctx.timestamp()).atZone(ZoneId.systemDefault()).withHour(0).plusDays(1).toInstant().toEpochMilli();
						ctx.timerService().registerEventTimeTimer(nextTime);
					}
				});
			}
		}
	}

	/**
	 * # 时间概念与Watermark
	 */
	static class TimeCharacteristicAndWatermark{
		//新版flink默认就是EventTime，不需要改了
		void setGlobalTimeCharacteristic(){
			StreamExecutionEnvironment executionEnvironment =
					StreamExecutionEnvironment.getExecutionEnvironment();
			executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		}
		//## 相关API的使用（两种生成方式）
		void primary(){
			//1. 在Source Function中直接定义Timestamps 和 Watermarks
			streamEnv.addSource(new SourceFunction<PageEvent>() {
				@Override
				public void run(SourceContext<PageEvent> ctx) throws Exception {
					FlinkSourceDataUtils.PAGEEVENTS.forEach(item -> {
						//告诉flink输入的数据的哪个字段是EventTime(指定时间戳字段，应该是秒为单位的long,建议使用primary type)
						ctx.collectWithTimestamp(item, item.getTimestamp());
						//告诉flink时间到达的最大延迟，过了这个时间就不管了，先处理数据吧
						//为啥指定过去的时间点？
						ctx.emitWatermark(new Watermark(item.getTimestamp() - 3/*水位3秒*/));
					});
					//设置默认的watermark,最大值表示关闭水位
					ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
				}
				@Override
				public void cancel() { }
			});
			//2. 通过Flink自带的Timestamp Assigner 指定 Timestamp 和生成Watermark
			/*
			- 使用场景：上述的 SourceFunction 数据源连接器如果是Flink中已经定义好的，无法进行改写，此时就需要借助Timestamp Assigner来管理数据流中的Timestamp元素和watermark
			- TimeAssigner操作通常跟在DataSource算子后面指定，也可以放在后面的，但必须置于第一个涉及时间操作的算子operator之前
			- Time Assigner会覆盖Source Function中指定的Timestamp和Watermark
			- Watermarks根据生成形式分为两种：
				+ Periodic Watermarks（AssignerWithPeriodicWatermarks）: 根据 设定的时间间隔 生成watermarks
				+ Punctuated Watermarks（）: 根据 设定的数据数量 生成watermarks
			 */
			DataStreamSource<PageEvent> pageEventDataSource = streamEnv.fromCollection(FlinkSourceDataUtils.PAGEEVENTS);
			//这个被废弃的API是理想状态下使用的递增时间戳生成watermarks策略，要求数据到达有序或者kafka分区内部有序（这样flink能保证union/connect后也是有序的，官网上说）
			pageEventDataSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PageEvent>(){
				@Override
				public long extractAscendingTimestamp(PageEvent element) {
					return element.getTimestamp();
				}
			});
			//生成环境不会使用理想环境下的编程思路
			Time secondsLag = Time.seconds(5); //注意这个Time类的位置：flink.streaming.api.windowing.time
			new BoundedOutOfOrdernessTimestampExtractor<PageEvent>(secondsLag){
				@Override
				public long extractTimestamp(PageEvent element) {
					return element.getTimestamp();
				}
			};
			//下面两个是官网的案例
			new AssignerWithPeriodicWatermarks<PageEvent>(){
				private final long maxOutofOrderness = 3500;//3.5秒
				private long currentMaxTimestamp;
				@Override
				public long extractTimestamp(PageEvent element, long recordTimestamp) {
					long timestamp = element.getTimestamp();
					currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
					return timestamp;
				}
				@Nullable
				@Override
				public Watermark getCurrentWatermark() {
					return new Watermark(currentMaxTimestamp - maxOutofOrderness);
				}
			}; //过期
			new AssignerWithPeriodicWatermarks<PageEvent>(){
				private final long maxOutofOrderness = 3500;//3.5秒
				@Override
				public long extractTimestamp(PageEvent element, long recordTimestamp) {
					return element.getTimestamp();
				}
				@Nullable
				@Override
				public Watermark getCurrentWatermark() {
					return new Watermark(System.currentTimeMillis() - maxOutofOrderness);
				}
			};//API 过期
			new AssignerWithPunctuatedWatermarks<PageEvent>(){
				@Override
				public long extractTimestamp(PageEvent element, long recordTimestamp) {
					return element.getTimestamp();
				}
				@Nullable
				@Override //决定是否使用水位机制,flink会先调用extractTimestamp方法，返回结果传入此方法决定是否使用watermark
				public Watermark checkAndGetNextWatermark(PageEvent lastElement, long extractedTimestamp) {
					return lastElement.getUserId().contains("in balcklist")?new Watermark(extractedTimestamp):null;
				}
			}; //过期
			//自定义watermarks生成策略，上面两个API过期了，1.13.5使用这一个 https://blog.csdn.net/Vector97/article/details/110150925
			WatermarkStrategy<PageEvent> watermarkStrategy =
					WatermarkStrategy.<PageEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withIdleness(Duration.ofMinutes(1))
					.withTimestampAssigner((element, recordTimestamp) -> {return 1;});

			pageEventDataSource.assignTimestampsAndWatermarks(watermarkStrategy);
		}

		public static void main(String[] args) {

		}
	}

	/**
	 * # window窗口计算
	 */
	static class AboutWindowsCalculator{
		static DataStreamSource<PageEvent> pageEventDataSource = streamEnv.fromCollection(FlinkSourceDataUtils.PAGEEVENTS);

		//Windows Assigner 如何使用
		static void fourKindsOfWindowsAPI(){
			KeyedStream<PageEvent, String> keyedStream = pageEventDataSource.keyBy(new KeySelector<PageEvent, String>() {
				@Override
				public String getKey(PageEvent value) throws Exception {
					return value.getUserId();
				}
			});
			//AllWindowedStream<PageEvent, Window> allWindowedStream = pageEventDataSource.windowAll(...);

			//滚动窗口处理5分钟内的数据 TumblingEventTimeWindows/TumblingProcessTimeWindows
			keyedStream.window(TumblingEventTimeWindows.of(Time.minutes(5)))
				.process(new ProcessWindowFunction<PageEvent, Integer, String, TimeWindow>() {
					@Override
					public void process(String s, Context context, Iterable<PageEvent> elements, Collector<Integer> out) throws Exception {
						//???
					}
				});
			//timeWindow api已废弃
			keyedStream.timeWindow(Time.seconds(10)).process(null);

			//滑动窗口:每50秒统计30分钟内的事件，对应的SlidingProcessingTimeWindows用法类似
			keyedStream.window(SlidingEventTimeWindows.of(Time.minutes(30), Time.seconds(50)))
					.process(null);
			//这种timeWindow需要指定UTC时差，国内是Time.hours(-8)，所以api废弃了
			keyedStream.timeWindow(Time.minutes(30),Time.seconds(50)).process(null);

			//会话窗口：
			keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(10))).process(null);
			//session gap可以动态调整
			keyedStream.window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<PageEvent>() {
				@Override
				public long extract(PageEvent element) {
					//输入性质的页面gap设置地长一点
					return element.getOperationType().startsWith("input") ? 50 : 10;
				}
			}));

			//全局窗口：
			keyedStream.window(GlobalWindows.create()).process(null);
		}

		//Trigger的使用
		static void abountTriggers(){
			KeyedStream<PageEvent, String> keyedStream = pageEventDataSource.keyBy(PageEvent::getUserId);
			keyedStream.window(EventTimeSessionWindows.withGap(Time.minutes(2))).trigger(ContinuousEventTimeTrigger.of(Time.minutes(5)));
			/*
			自定义窗口触发器
			先了解每个方法返回的TriggerResult：TriggerResult是一个枚举
			CONTINUE(false, false) 不触发计算，不触发清理，继续等待
			FIRE(true, false) 触发计算，但数据保留
			PURGE(false, true) 清理掉数据，不触发计算
			FIRE_AND_PURGE(true, true) 触发计算后清除对应数据

			需求描述：窗口是SessionWindow时，如果用户长时间不停操作，导致session gap一直都不生成，该用户的数据长期存储在窗口中，希望每隔5分钟统计下窗口的结果，清除掉数据
			其实这就是Flink封装的ContinuousEventTimeTrigger的功能
			 */
			keyedStream.window(EventTimeSessionWindows.withGap(Time.minutes(2))).trigger(MyContinuousEventTimeTrigger.of(Time.seconds(12)));
		}
		/**
		 * 仿照 ContinuousEventTimeTrigger 实现一个自定义的Trigger（未测试）
		 * 需要实现的方法很多
		 * 自定义触发器会覆盖默认触发器中的行为，如EventTimeWindow对应的默认触发器 EventTimeTrigger，需要确保不遗漏默认触发器中的watermark处理代码
		 * GlobalWindow的默认触发器是NeverTrigger，所以需要自定义触发器确定究竟何时触发，否则会堆积数据直至内存溢出
		 */
		public static class MyContinuousEventTimeTrigger extends Trigger<PageEvent, Window> {
			private long interval;//单位毫秒
			public MyContinuousEventTimeTrigger(long interval) {
				this.interval = interval;
			}
			public static MyContinuousEventTimeTrigger of(Time interval){
				return new MyContinuousEventTimeTrigger(interval.toMilliseconds());
			}

			private StateDescriptor<ReducingState<Long>,Long> stateDescriptor = new ReducingStateDescriptor<Long>(
					"current_partition_starttime", (ReduceFunction<Long>) (v1, v2) -> Math.min(v1,v2), org.apache.flink.api.common.typeinfo.Types.LONG);

			@Override //针对每一个接入窗口的数据元素进行触发操作,每一个元素过来都会被调用到
			public TriggerResult onElement(PageEvent pageEvent, long timestamp, Window window, TriggerContext context) throws Exception {
				//获取当前分区中的最小的时间戳,就是触发窗口计算的时间戳
				ReducingState<Long> fireTimestampState = context.getPartitionedState(stateDescriptor);
				Long fireTimestamp = fireTimestampState.get();
				//如果当前watermark超过窗口的结束时间，则清除定时器内容，直接出发窗口计算
				if (window.maxTimestamp() <= context.getCurrentWatermark()){
					//clearTimeeForState(context);
					if (fireTimestamp != null){
						context.deleteEventTimeTimer(fireTimestamp);
					}
					return TriggerResult.FIRE;
				}else {
					//将窗口的结束时间注册到EventTime定时器
					context.registerEventTimeTimer(window.maxTimestamp());
					//第一次执行时fire时间戳不存在，需要
					if (fireTimestamp == null){
						/*
						给出19700101到现在的时间戳（假定周期开始时间19700101），执行周期interval，计算下一次触发时间戳
						19700101 -=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=Now
						|----|----|----|--**interval**--|----|----|--- |<-- 这根线是下一次执行timestamp，如何计算nextFireTimestamp
						0 -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=16xxxx(timestamp)
						 */
						long nextFireTimestamp = timestamp - (timestamp % interval) + interval;
						context.registerEventTimeTimer(nextFireTimestamp);
						fireTimestampState.add(nextFireTimestamp);
					}
					return TriggerResult.CONTINUE;
				}
			}
			@Override //根据接入窗口的ProcessingTime进行触发，window传入的Windows Assigner是EventTimeXXXWindow时，不会基于ProcessingTime触发，直接返回CONTINUE
			public TriggerResult onProcessingTime(long timestamp, Window window, TriggerContext context) throws Exception {
				return TriggerResult.CONTINUE;
			}
			@Override //watermark超过注册时间时，会执行onEventTime方法(此实现类应付场景是每 interval(12s)统计过去 window(2分钟)的数据)
			public TriggerResult onEventTime(long timestamp, Window window, TriggerContext context) throws Exception {
				ReducingState<Long> partitionedState = context.getPartitionedState(stateDescriptor);
				Long removingTimestamp = partitionedState.get();
				//watermark水位满了必须执行并清理
				if (timestamp == window.maxTimestamp()){
					//触发执行前需要移除之前的窗口，onElement时会再重新注册初始timer
					Optional.ofNullable(removingTimestamp).ifPresent(context::deleteEventTimeTimer);
					return TriggerResult.FIRE;
				} else {
					//刚好到达interval间隔的时间戳，时间窗口内的周期执行，注册下一个interval时间戳，同时执行
					if (removingTimestamp != null && timestamp == removingTimestamp){
						partitionedState.clear();
						long nextPeriodEndTime = timestamp + interval;
						partitionedState.add(nextPeriodEndTime);
						context.registerEventTimeTimer(nextPeriodEndTime);
						return TriggerResult.FIRE;
					}else {
						//会走到这里？
						return TriggerResult.CONTINUE;
					}
				}
			}
			@Override //窗口状态merge的逻辑
			public void onMerge(Window window, OnMergeContext context) throws Exception {
				context.mergePartitionedState(stateDescriptor);
				Long nextFireTimestamp = context.getPartitionedState(stateDescriptor).get();
				Optional.ofNullable(nextFireTimestamp).ifPresent(context::registerEventTimeTimer);
			}
			@Override //窗口计算执行后的数据清理方法
			public void clear(Window window, TriggerContext context) throws Exception {
				context.deleteEventTimeTimer(window.maxTimestamp());
				ReducingState<Long> fireTimestampState = context.getPartitionedState(stateDescriptor);
				Optional.ofNullable(fireTimestampState.get()).ifPresent(item -> {
					context.deleteEventTimeTimer(item);
					fireTimestampState.clear();
				});
			}
			@Override //用于SessionWindows的merge，指定为可以merge
			public boolean canMerge() {
				return true;
			}
		}

		//Windows Function 的使用：窗口内的数据计算逻辑
		static void aboutWindowsFunction(){
			KeyedStream<PageEvent, String> keyedStream = pageEventDataSource.keyBy(PageEvent::getUserId);
			WindowedStream<PageEvent, String, TimeWindow> window = keyedStream.window(SlidingEventTimeWindows.of(Time.minutes(30), Time.seconds(30)));
			//Reduce Function: (T,T) -> T
			window.reduce(new ReduceFunction<PageEvent>() {
				@Override
				public PageEvent reduce(PageEvent value1, PageEvent value2) throws Exception {
					value1.setPageId(value1.getPageId()+","+value2.getPageId());
					return value1;
				}
			});
			//Aggregate Function: 对数据集中的字段求平均值
			window.aggregate(new AggregateFunction<PageEvent/*item*/, Pair<Long,Long>/*accumulator*/, Long/*result*/>() {
				@Override //总和初始值为0
				public Pair<Long,Long> createAccumulator() {
					return Pair.of(0L,0L);
				}
				@Override //accumulator聚合逻辑
				public Pair<Long,Long> add(PageEvent value, Pair<Long,Long> accumulator) {
					int add = value.getPageId().equals("shopping") ? 10 : 5;
					long left = accumulator.getLeft() + 1;
					long right = accumulator.getRight() + add;
					return Pair.of(left, right);
				}
				@Override //聚合完成后的操作
				public Long getResult(Pair<Long,Long> accumulator) {
					return accumulator.getRight() / (accumulator.getLeft() == 0 ? 1 : accumulator.getLeft());
				}
				@Override //分区集群执行结果的合并
				public Pair<Long,Long> merge(Pair<Long,Long> a, Pair<Long,Long> b) {
					long left = a.getLeft() + b.getLeft();
					long right = a.getRight() + b.getRight();
					return Pair.of(left, right);
				}
			});
			//ProcessWindow Function: 如果窗口内元素计算结果需要考虑窗口内的全部元素，就需要使用ProcessWindowFunction(尽量避免使用)
			//需求描述：统计中位数和众数
			window.process(new ProcessWindowFunction<PageEvent/*IN*/, String/*OUT*/, String/*Key*/, TimeWindow>() {
				@Override
				public void process(String key, Context context, Iterable<PageEvent> elements, Collector<String> out) throws Exception {
					Map<String/*pageId*/, Integer/*count*/> map = new HashMap<>();
					for (PageEvent element : elements) {
						Integer count = map.computeIfAbsent(element.getPageId(), xxx -> 0);
						count ++;
						map.put(element.getPageId(), count);
					}
					//...
					int result = 0;//max(map.value)
					out.collect(key + result + context.window().getEnd());
				}
			});
			//Incremental Aggregation 整合 ProcessWindowsFunction: 一次传两个函数
			//需求描述：求窗口中指标最大值(pageId最长的)以及对应窗口的终止时间
			window.reduce(new ReduceFunction<PageEvent>() {
				@Override
				public PageEvent reduce(PageEvent value1, PageEvent value2) throws Exception {
					return value1.getPageId().length() > value2.getPageId().length() ? value1 : value2;
				}
			}, new ProcessWindowFunction<PageEvent, String, String, TimeWindow>() {
				@Override
				public void process(String s, Context context, Iterable<PageEvent> elements, Collector<String> out) throws Exception {
					PageEvent next = elements.iterator().next();
					out.collect(context.window().getEnd() +","+ next.getPageId());
				}
			});
			//ProcessWindowFunction 针对指定的Key在窗口上存储
			/*
			需求描述：用户ID作为key，求每个用户ID最近1h的登录数，如果平台中一共有3000用户，则窗口计算会创建3000个窗口实例
			每个窗口实例都会保存每个key的状态数据，可以通过ProcessWindowFunction中的Context对象获取并操作Per-window State数据
			 */
			window.process(new ProcessWindowFunction<PageEvent, String, String, TimeWindow>(){
				@Override
				public void process(String s, Context context, Iterable<PageEvent> elements, Collector<String> out) throws Exception {
					//两种Per-window state的使用
					context.globalState();
					context.windowState();
				}
			});

		}

		//Evictor的使用
		static void abountEvictor(){
			KeyedStream<PageEvent, String> keyedStream = pageEventDataSource.keyBy(PageEvent::getUserId);
			keyedStream.window(null).evictor(DeltaEvictor.of(12.0, new DeltaFunction<PageEvent>() {
				@Override
				public double getDelta(PageEvent oldDataPoint, PageEvent newDataPoint) {
					return newDataPoint.getOperationType().length() - oldDataPoint.getOperationType().length();
				}
			}));
			//自定义一个Evictor after before的是对window讲的，而且在before/after中调整元素的顺序，并不能让元素进入window保持有序
			keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(20)))
					.evictor(new Evictor<PageEvent, TimeWindow>() {
						@Override
						public void evictBefore(Iterable<TimestampedValue<PageEvent>> elements, int size, TimeWindow window, EvictorContext context) {

						}
						@Override
						public void evictAfter(Iterable<TimestampedValue<PageEvent>> elements, int size, TimeWindow window, EvictorContext context) {

						}
					});

		}

		//AllowedLateness 和 SideOutput
		static void latenessSideoutput(){
			KeyedStream<PageEvent, String> keyedStream = pageEventDataSource.keyBy(PageEvent::getUserId);
			OutputTag lateDataOutputTag = new OutputTag("biz_code_topic_late_data");
			SingleOutputStreamOperator operator =
					keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(30)))
						.allowedLateness(Time.seconds(20))
						//为迟到的数据打标记
						.sideOutputLateData(lateDataOutputTag)
						.process(null);
			//使用标记获取迟到的数据
			DataStream lateDataStream = operator.getSideOutput(lateDataOutputTag);
			//将迟到的数据单独取出来存放
			lateDataStream.addSink(null);
		}

		//连续窗口计算
		static void continuousPaneCalculation(){
			KeyedStream<PageEvent, String> keyedStream = pageEventDataSource.keyBy(PageEvent::getUserId);
			//独立窗口计算：窗口间元素不相干
			SingleOutputStreamOperator<Object> operator1 =
					keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(20))).process(null);
			SingleOutputStreamOperator<Object> operator2 =
					keyedStream.window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10))).process(null);
			//连续窗口计算：上游pane的元素是下游pane的输入，窗口间信息共享
			/*
			需求描述：上游窗口统计最近10分种key的最小值，通过下游窗口统计整个窗口上TopK的值
			两个窗口的类型和EndTime一致，上游将窗口元数据watermark信息传递到下游窗口中，真正触发计算的是在下游窗口，窗口的计算结果全部在下游窗口中统计得出
			最终完成在同一个窗口中同时计算与key相关和非key相关的指标
			 */
			SingleOutputStreamOperator<PageEvent> operator = keyedStream
					.window(TumblingEventTimeWindows.of(Time.minutes(10)))
					.reduce((ReduceFunction<PageEvent>) (value1, value2) ->
							value1.getUserId().length() < value2.getUserId().length() ? value1 : value2);
			operator.windowAll(TumblingEventTimeWindows.of(Time.minutes(10)))
					.process(new ProcessAllWindowFunction<PageEvent,String, TimeWindow>(){
						@Override
						public void process(Context context, Iterable<PageEvent> elements, Collector<String> out) throws Exception {
							elements.forEach(item -> {
								//if ...
								out.collect(item.getUserId());
							});
						}
					});
		}
	}

	//多流合并计算
	static class AbountStreamJoin{
		static DataStreamSource<PageEvent> pageEventDataSource = streamEnv.fromCollection(FlinkSourceDataUtils.PAGEEVENTS);
		static DataStreamSource<ViewEvent> viewEventDataSource = streamEnv.fromCollection(FlinkSourceDataUtils.VIEWEVENTS);
		/*
		需求描述：有两个流，数据有重复，但字段部分重合，但key名称不一致，需要inner join一下，去除重复的，join成一个流统计下
		 */
		static void multiStream(){
			KeyedStream<PageEvent, String> pageEventStream = pageEventDataSource.keyBy(PageEvent::getUserId);
			KeyedStream<ViewEvent, String> viewEventStream = viewEventDataSource.keyBy(ViewEvent::getUserNo);
			//滚动窗口关联,元素不会重复，内连接，关联不到就不输出
			pageEventStream.join(viewEventStream)
							.where(PageEvent::getUserId)
							.equalTo(ViewEvent::getUserNo)
							.window(TumblingEventTimeWindows.of(Time.seconds(10)))
							.apply((page,view) -> new Tuple2(page.getUserId(),page.getPageId()+","+view.getPageId()));
			//滑动窗口 window function 换成SlidingEventTimeWindows
			//会话窗口关联 与此类似
			//间隔关联
			pageEventStream.intervalJoin(viewEventStream)
					//每个pageEvent元素到达时间前2秒和后4秒之间到达的viewEvent被关联进来
					.between(Time.seconds(2),Time.seconds(4))
					.process(new ProcessJoinFunction<PageEvent, ViewEvent, String>() {
						@Override
						public void processElement(PageEvent left, ViewEvent right, Context ctx, Collector<String> out) throws Exception {
							out.collect(ctx.getTimestamp()+":"+left.getPageId()+"-"+right.getPageId());
						}
					});
		}



	}

	//Flink有状态计算
	static class StatefulCalculation{
		static DataStreamSource<PageEvent> pageEventDataSource = streamEnv.fromCollection(FlinkSourceDataUtils.PAGEEVENTS);

		//[3304]需求描述：通过valueState计算最小值, ValueStat#value() 获取值；ValueStat#update() 修改值
		static void valueState(){
			pageEventDataSource.keyBy(PageEvent::getUserId)
					.flatMap(new RichFlatMapFunction<PageEvent, Pair<String,Long>>() {
						private ValueState<Long> minmumValueStat;

						@Override
						public void open(Configuration parameters) throws Exception {
							//创建ValueStateDescriptor
							ValueStateDescriptor<Long> minimumDescriptor = new ValueStateDescriptor<>("计算最小值", Long.class);
							//Querable State 将状态变量暴露出来允许业务系统查询 [3305]
							minimumDescriptor.setQueryable("minmum_querable_state");
							//通过RuntimeContext拿到Stat:
							minmumValueStat = getRuntimeContext().getState(minimumDescriptor);
							//getRuntimeContext().getReducingState(new ReducingStateDescriptor<?>(...))
							//getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("用户1天的统计量",String.class, Long.class));
						}
						@Override
						public void close() throws Exception {
							//实现一个生命周期结束的close方法
						}
						@Override
						public void flatMap(PageEvent input, Collector<Pair<String,Long>> out) throws Exception {
							//获取当前最小值，进行更新
							Long minmum = minmumValueStat.value();
							if (input.getScore() > minmum){
								out.collect(Pair.of(input.getUserId(), minmum));
							}else {
								minmumValueStat.update(input.getScore());
								out.collect(Pair.of(input.getUserId(), input.getScore()));
							}
						}
					});
		}/*
		-	RichXXXFunction与XXXFunctioon的区别：
		Rich能够操作状态，继承了AbstractRichFunction获取了getRuntimeContext和getIterationRuntimeContext两个能力，可以操作State变量
		-   上述操作状态变量的方式在scala api中的写法非常简洁
		pageEventDataSource.keyBy(PageEvent::getUserId)后可以调 flatMapWithState获取状态，系统自动创建count对应的状态来存储每次更新的累加值

		*/

		//[3305]State生命周期: 同样需要使用Descriptor
		static void stateTTL(){
			StateTtlConfig stateTtlConfig = StateTtlConfig
					//指定TTL时长
					.newBuilder(org.apache.flink.api.common.time.Time.seconds(10))
					/*
					  指定TTL刷新策略只对创建和写入操作有效
						StateTtlConfig.UpdateType 的介绍：
						OnCreateAndWrite 创建和写入时更新TTL
						OnReadAndWrite 比OnCreateAndWrite多一个，读取时也更新TTL
						如果一个状态指标一直没有被使用，TTL一直不更新，导致未清理，导致系统中状态数据越来越多，此时可以考虑使用清理策略 cleanupFullSnapshot
						但这个策略不适合用于RocksDB做增量Checkpointing操作
					 */
					.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).cleanupFullSnapshot()
					/*
					  指定状态可见性
						StateTtlConfig.StateVisibility 枚举介绍
						ReturnExpiredIfNotCleanedUp 状态数据即使过期但没有清理仍然返回
						NeverReturnExpired 过期了就不返回
					 */
					.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
					.build();
			ValueStateDescriptor<Long> valueStateDescriptor = new ValueStateDescriptor<Long>("valueState生命周期", Long.class);
			valueStateDescriptor.enableTimeToLive(stateTtlConfig);
		}

		//展示下Managed Operator State是如何使用的
		//3306 需求描述：实现CheckpointedFunction接口利用Operator State统计输入到算子的数据量
		static class CheckpointCount implements
				FlatMapFunction<Tuple2<Integer,Long>, Tuple3<Integer,Long,Long>>,
				CheckpointedFunction {
			private Long count;
			private ValueState<Long> valueState;
			private ListState<Long> listState;
			@Override
			public void flatMap(Tuple2<Integer, Long> value, Collector<Tuple3<Integer, Long, Long>> out) throws Exception {
				Long keyedCount = valueState.value() + 1;
				valueState.update(keyedCount);
				count++;
				out.collect(Tuple3.of(value.f0, keyedCount, count));
			}
			//当发生snapshot时，将count添加到listState中
			@Override
			public void snapshotState(FunctionSnapshotContext context) throws Exception {
				//清理掉上一次checkpoint中存储的operatorState的数据
				listState.clear();
				//添加并更新本次算子中需要创建checkpoint的operatorCount状态变量
				listState.add(count);
			}
			//系统重启时会调用这里的initializeState方法，重新恢复keyedState和OperatorState状态变量
			@Override
			public void initializeState(FunctionInitializationContext context) throws Exception {
				//从context中拿ValueState
				KeyedStateStore keyedStateStore = context.getKeyedStateStore();
				ValueStateDescriptor<Long> valueStateDescriptor =
						new ValueStateDescriptor<Long>("KeyedStateDestor",TypeInformation.of(Long.class));
				valueState = keyedStateStore.getState(valueStateDescriptor);
				//从context中拿ListState
				OperatorStateStore operatorStateStore = context.getOperatorStateStore();
				ListStateDescriptor<Long> operatorStateDestor =
						new ListStateDescriptor<>("OperatorStateDestor", TypeInformation.of(Long.class));
				/*
				对于ListState数据有一个恢复时的分区重分布策略 [3307]
				getListState表示采用默认的 Even-split Redistribution策略
				如果要使用 Union Redistribution 策略，可以通过getUnionListState来获取
				 */
				listState = operatorStateStore.getListState(operatorStateDestor);
				//定义在restore过程中，从listState中恢复数据的逻辑
				if (context.isRestored()){
					Iterator<Long> iterator = listState.get().iterator();
					Long countRestore = 0L;
					while (iterator.hasNext()){
						countRestore += iterator.next();
					}
					count = countRestore;
				}
			}
		}

		// 需求描述，上面是实现了 CheckpointedFunction 操作了Operator State
		//如何通过 ListCheckpointed 操作 Operator State
		//ListCheckpointed 已废弃，官方认为应使用本就很少使用的CheckpointedFunction

		static void checkpoint(){
			streamEnv.enableCheckpointing(1000);
			CheckpointConfig checkpointConfig = streamEnv.getCheckpointConfig();
			checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
			//处理Checkpoint的超时时间，超过时会中断Checkpoint过程，默认10分钟
			checkpointConfig.setCheckpointTimeout(60000);
			//设置两个checkpoint之间的最小时间间隔，防止出现状态数据过大checkpoint执行时间过长，导致Checkpoint积压过多，最终Flink密集触发Checkpoint操作
			checkpointConfig.setMinPauseBetweenCheckpoints(500);
			//设置最大并行执行的检查点数量，默认1个
			checkpointConfig.setMaxConcurrentCheckpoints(1);
			//设置周期性的外部检查点，将状态数据持久化到外部系统中，这种方式不会在任务停止的过程中清理掉检查点数据，而是保存到外部系统介质中
			checkpointConfig.enableExternalizedCheckpoints(
					CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
			checkpointConfig.setTolerableCheckpointFailureNumber(5);
		}

		static void querableState() {
			SingleOutputStreamOperator<Tuple2<String, Long>> operator = streamEnv.fromCollection(FlinkSourceDataUtils.PAGEEVENTS)
					.map(item -> new Tuple2<String, Long>(item.getUserId(), item.getScore()))
					.keyBy(item -> item.f0)
					.window(TumblingEventTimeWindows.of(Time.seconds(5)))
					.max("f1");
			operator.keyBy(item -> item.f0).asQueryableState("maxInputState");
			//DataStream API设置 QuerableState 后将不能再接入后续DataSink

			//DataStream 结合 ValueStateDescriptor 设置querable state
			TypeInformation<Tuple2<String, Long>> typeInforTuple2 = TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
			});
			TupleTypeInfo<Tuple> tupleTypeInfo = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(String.class, Long.class);
			operator.keyBy(item -> item.f0).asQueryableState("maxInputState",
					new ValueStateDescriptor<Tuple2<String, Long>>("maxInputState", typeInforTuple2)
			);
		}
	}

	//Flink DataSet API
	static class AboutDataSetAPI{
		private static final String driverName = "com.mysql.cj.jdbc.Driver";
		private static final String dbURL = "jdbc:mysql://localhost:3306/sakila";
		private static final String userName = "root";
		private static final String passcode = System.getenv("passcode");

		//实现一个简单的单词计数功能（防止泛型擦除，不要使用lambda表达式）
		static void simpleWordCount() throws Exception {
			DataSource<String> dataSource =
					batchEnv.fromElements("aab csi sdof soi", "sdof uitd ci sdof","sdof uitd aab");
			//这里不能使用Lambda表达式，泛型擦除会导致类型信息丢失
			FlatMapOperator<String, String> flatMapOperator = dataSource.flatMap(
					new FlatMapFunction<String, String>() {
						@Override
						public void flatMap(String value, Collector<String> out) throws Exception {
							String[] ss = value.split(" ");
							//Stream.of(ss).forEach(out::collect);
							for (String s : ss){
								out.collect(s);
							}
						}
					});
			//由于泛型擦除的原因，这里写 flatMapOperator.filter((FilterFunction<String>)StringUtils::isNotBlank) 会报错
			//InvalidTypesException: The return type of function 'simpleWordCount(FlinkPrimaryAPI.java:1089)' could not be determined automatically, due to type erasure. You can give type information hints by using the returns(...) method on the result of the transformation call, or by letting your function implement the 'ResultTypeQueryable' interface.
			FilterOperator<String> filterOperator = flatMapOperator.filter(new FilterFunction<String>() {
				@Override
				public boolean filter(String value) throws Exception {
					return StringUtils.isNotBlank(value);
				}
			});
			MapOperator<String, Tuple2<String,Integer>> mapOperator = filterOperator.map(
					new MapFunction<String, Tuple2<String, Integer>>() {
						@Override
						public Tuple2<String, Integer> map(String value) throws Exception {
							return new Tuple2<>(value, 1);
						}
					});
			UnsortedGrouping<Tuple2<String, Integer>> unsortedGrouping = mapOperator.groupBy("f0");
			// dataset API：这里不能使用KeySelector，否则报 Aggregate does not support grouping with KeySelector functions, yet.
			//UnsortedGrouping<Tuple2<String, Integer>> unsortedGrouping =
			//		mapOperator.groupBy(new KeySelector<Tuple2<String, Integer>, String>() {
			//			@Override
			//			public String getKey(Tuple2<String, Integer> value) throws Exception {
			//				return value.f0;
			//			}
			//		});

			/**
			 * 累加得到最终结果，下面两行写法等效
			 */
			AggregateOperator<Tuple2<String, Integer>> aggregateOperator = unsortedGrouping.aggregate(Aggregations.SUM,1);
			//AggregateOperator<Tuple2<String, Integer>> aggregateOperator = unsortedGrouping.sum(1);
			//打印结果
			aggregateOperator.print();
			//dataset api 不需要调execute，否则报错
			//batchEnv.execute();
		}

		/**
		 * DataSource数据接入
		 DataSet API支持从多种数据源中将批量数据集读到Flink系统中，并转成DataSet数据集
		 */
		static class DataSourcesAPI{
			private static final String update = "update city set city = ? where city_id = ?";
			//读取本地文件 - filePath = file:///file/mars_mobile_page.csv
			//HDFS文件 - filePath = hdfs://nnHost:nnPort/path/textFile
			static void fileDataSource(String filePath) throws Exception{
				DataSource<String> stringDataSource = batchEnv.readTextFile(filePath);
				stringDataSource.print();
				/*
				StringValue是一种可变的String类型，通过StringValue存储文本可以有效降低String对象创建数量，从而提升性能
				 */
				DataSource<StringValue> stringValueDataSource = batchEnv.readTextFileWithValue(filePath);
				/*
				CsvReader自带转Tuple功能，很方便
				 */
				DataSource<Tuple4<Long, String, Long, String>> types = batchEnv.readCsvFile(filePath).fieldDelimiter(",").types(
						Long.class, String.class, Long.class, String.class);
				MapOperator<Tuple4<Long, String, Long, String>, Long> mapOperator = types.map(new MapFunction<Tuple4<Long, String, Long, String>, Long>() {
					@Override
					public Long map(Tuple4<Long, String, Long, String> value) throws Exception {
						return value.f2;
					}
				});
				mapOperator.print();
			}
			//集合类数据 fromCollection fromElements
			static void collectionDataSource()throws Exception{
				//凭空造数据
				DataSource<Long> longDataSource = batchEnv.generateSequence(1, 200);
			}
			static void inputFormatDataSource(String filePath) throws Exception{
				batchEnv.readFile(new FileInputFormat<String>() {
					@Override
					public boolean reachedEnd() throws IOException {
						return false;
					}
					@Override
					public String nextRecord(String reuse) throws IOException {
						return null;
					}
				},filePath);
				//从mysql中读取数据,使用createInput
			}
			/*
			从MySQL读取/写入数据
			如果使用flink封装的 JDBCInputFormat， 需要引入org.apache.flink flink-jdbc_xxx
			也可以通过RichSourceFunction自定义数据源
			 */
			static DataSource<Row> readFromMySQL() throws Exception{
				String query = "select * from city where country_id=2";
				TypeInformation[] fieldTypes = new TypeInformation[]{
						TypeInformation.of(Long.class),TypeInformation.of(String.class),
						TypeInformation.of(Long.class),TypeInformation.of(Date.class)
				};
				RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
				//JDBC链接信息Builder的两种创建方式
				JDBCInputFormat.JDBCInputFormatBuilder inputBuilder = JDBCInputFormat.buildJDBCInputFormat();
				//JDBCInputFormat.JDBCInputFormatBuilder jdbcInputFormatBuilder = new JDBCInputFormat.JDBCInputFormatBuilder();
				JDBCInputFormat jdbcInputFormat = inputBuilder.setDrivername(driverName).setDBUrl(dbURL)
						.setUsername(userName).setPassword(passcode)
						.setQuery(query).setRowTypeInfo(rowTypeInfo)
						.finish();
				DataSource<Row> dataSource = batchEnv.createInput(jdbcInputFormat);
				return dataSource;
			}
			//实现一种跑批功能，对查到的数据做一点修改后入库
			static void write2MySQLWithRichOutputFormat(DataSource<Row> dataSource) throws Exception{
				MapOperator<Row, Tuple2<String,Integer>> mapOperator = dataSource.map(new MapFunction<Row, Tuple2<String,Integer>>() {
					@Override
					public Tuple2<String,Integer> map(Row row) throws Exception {
						//Set<String> fieldNames = row.getFieldNames(true);//拿到一堆f0 f1... 拿不到MySQL列名
						String city = row.<String>getFieldAs(1);
						Integer city_id = row.<Integer>getFieldAs(0);//dataset api不成熟，传进去Long.class得到却是Integer，转换会报错
						System.out.println(row.<Date>getFieldAs(3));
						if (city.endsWith("2")){
							city = city.substring(0,city.length()-1);
						}else {
							city = city+"2";
						}
						return new Tuple2<String,Integer>(city,city_id);
					}
				});
				//写入数据库的操作可以通过自定义RichOutputFormat的方式实现(仅仅是程序能跑而已)
				DataSink<Tuple2<String, Integer>> dataSink = mapOperator.output(new RichOutputFormat<Tuple2<String, Integer>>() {
					private Connection connection;
					private PreparedStatement statement;

					@Override
					public void configure(Configuration parameters) {
					}

					@Override
					public void open(int taskNumber, int numTasks) throws IOException {
						try {
							Class.forName(driverName);
							connection = DriverManager.getConnection(dbURL, userName, passcode);
							statement = connection.prepareStatement(update);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}

					@Override
					public void writeRecord(Tuple2<String, Integer> record) throws IOException {
						try {
							statement.setString(1, record.f0);
							statement.setInt(2, record.f1);
							int i = statement.executeUpdate();
							System.out.println(record.f0 + "更新成功：" + (i > 0));
						} catch (SQLException throwables) {
							throwables.printStackTrace();
						}

					}

					@Override
					public void close() throws IOException {
						try {
							statement.close();
							connection.close();
						} catch (SQLException throwables) {
							throwables.printStackTrace();
						}
					}
				});
				batchEnv.execute();
			}
			//使用 JDBCOutputFormat 如何将数据落库
			//用于insert数据，传入的SQL通常就是insert into xxx value xxx
			static void insert2MySQL() throws Exception{
				DataSource<Row> dataSource = readFromMySQL();//懒得造数据了，直接查出来，改下city_id/city落库
				//JDBCOutputFormat中将数据类型限定为Row！
				MapOperator<Row, Row> mapRowOperator = dataSource.map(new MapFunction<Row, Row>() {
					@Override
					public Row map(Row row) throws Exception {
						String city = row.<String>getFieldAs(1);
						if (city.endsWith("2")) {
							city = city.substring(0, city.length() - 1);
						} else {
							city = city + "2";
						}
						row.setField(1, city);
						return row;
					}
				});
				JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder = JDBCOutputFormat.buildJDBCOutputFormat()
						.setDrivername(driverName).setDBUrl(dbURL)
						.setUsername(userName).setPassword(passcode)
						.setBatchInterval(2);//一次批量提交多少条数据
				//这里会出key冲突问题，意思一下
				OutputFormat outputFormat = outputBuilder.setQuery("insert into city(city_id,city,country_id,last_update) value(?,?,?,?)").finish();
				mapRowOperator.output(outputFormat);
				//将Tuple类型结果写入关系型数据库
				batchEnv.execute();
			}
			//更新数据，重新创建出Row，这种写法因为类型丢失的问题，运行时有很多WARN，虽然最终成功更新数据库
			//看框架源码 org.apache.flink.api.java.io.jdbc.JDBCTypeUtil 可知，可以向 JDBCOutputFormat 传递类型信息来避免类型映射告警
			static void updateMySQL() throws Exception{
				DataSource<Row> rowDataSource = readFromMySQL();
				MapOperator<Row, Row> mapOperator = rowDataSource.map(new MapFunction<Row, Row>() {
					@Override
					public Row map(Row row) throws Exception {
						String city = row.<String>getFieldAs(1);
						Integer city_id = row.<Integer>getFieldAs(0);
						if (city.endsWith("2")) {
							city = city.substring(0, city.length() - 1);
						} else {
							city = city + "2";
						}
						//Row存在类型丢失问题 class org.apache.flink.types.Row is missing a default constructor so it cannot be used as a POJO type and must be processed as GenericType. Please read the Flink documentation on "Data Types & Serialization" for details of the effect on performance.
						return Row.of(city, city_id);
					}
				});
				int[] types = {Types.VARCHAR, Types.INTEGER};
				JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder = JDBCOutputFormat.buildJDBCOutputFormat()
						.setDrivername(driverName).setDBUrl(dbURL)
						.setUsername(userName).setPassword(passcode)
						.setSqlTypes(types) //JDBCInputFormat中也有类型设置，少这一行设置会有很多类型问题WARN
						.setBatchInterval(2);//一次批量提交多少条数据
				OutputFormat outputFormat = outputBuilder.setQuery(update).finish();
				DataSink dataSink = mapOperator.output(outputFormat);
				batchEnv.execute();
			}
			public static void main(String[] args)throws Exception {
				//本地调试发现这样读取文件才好用
				URL csvResource = FlinkPractice.class.getResource("/file/mars_mobile_page.csv");
				//fileDataSource(csvResource.getPath());
				updateMySQL();
			}
		}

		//# DataSetAPI中丰富的转换操作
		static class DateSetOperation{
			//Map操作，数据分区不发生变化
			static void operate() throws Exception{
				DataSource<Long> longDataSource = batchEnv.generateSequence(1, 100);
				//MapPartiton 不再是1转1，而是一个分区中的 n转n：通过Iterator传入，通过collector收集
				longDataSource.mapPartition(new MapPartitionFunction<Long, String>() {
					@Override
					public void mapPartition(Iterable<Long> values, Collector<String> out) throws Exception {
					}
				});
				//ReduceGroup 将一组元素合并成一个或多个元素，可以在整个数据集上使用，也可以用于 Group Data Set
				GroupReduceOperator<Long, String> groupReduceOperator = longDataSource.reduceGroup(new GroupReduceFunction<Long, String>() {
					@Override
					public void reduce(Iterable<Long> values, Collector<String> out) throws Exception {
						Stream<Long> stream = StreamSupport.stream(values.spliterator(), false);
						LongSummaryStatistics summary = stream.collect(Collectors.summarizingLong(item -> item));
						out.collect("count = "+summary.getCount());
						out.collect("sum = "+summary.getSum());
						out.collect("average = "+summary.getAverage());
						out.collect("time = "+new Date());
					}
				});
				groupReduceOperator.print();
			}
			static void aggregate() throws Exception{
				DataSource<Tuple2<Long, Long>> tuple2DataSource =
						batchEnv.<Tuple2<Long, Long>>fromElements(new Tuple2<>(12L, 15L), new Tuple2<>(13L, 18L), new Tuple2<>(13L,17L));
				//两个aggregateFunction连用，先求第一个字段最大的，得到两个结果，在从两个结果中找第二个字段最小的，最终
				AggregateOperator<Tuple2<Long, Long>> aggregateOperator =
						tuple2DataSource.aggregate(Aggregations.MAX, 0)
								.aggregate(Aggregations.MIN, 1);
				aggregateOperator.print();//(13,17)
			}
			//Dataset 可以去重
			static void distinct() throws Exception{
				DataSource<Integer> dataSource = batchEnv.<Integer>fromElements(12, 13, 15, 13, 14);
				DistinctOperator<Integer> distinctOp = dataSource.distinct();
				distinctOp.print();
			}
			static DataSource<Tuple3<Long, String, Integer>> students = batchEnv.<Tuple3<Long, String, Integer>>fromElements(
					new Tuple3<>(1L, "jack", 12),
					new Tuple3<>(3L, "lucy", 13),
					new Tuple3<>(4L, "daniel", 13)
			);
			static DataSource<Tuple3<Long, String, String>> address = batchEnv.<Tuple3<Long, String, String>>fromElements(
					new Tuple3<>(1L, "china", "beijing"),
					new Tuple3<>(2L, "china", "shanghai"),
					new Tuple3<>(3L, "america", "new york")
			);
			//多表关联
			static void tryJoin() throws Exception{
				//join操作，左边的表在where中指定字段，右边的表在equalTo中指定. flink中默认的join是 inner join
				//也可以使用KeySelector选择key
				/*
				使用flink java api 模拟SQL中的联表查询
				select s.id,s.name,a.city from student s inner join address a on a.student_id = s.id
				 */
				JoinOperator.DefaultJoin<Tuple3<Long, String, Integer>,
						Tuple3<Long, String, String>> joinOperator =
						students.join(address).where("f0").equalTo(0);
				JoinFunction<Tuple3<Long, String, Integer>,
						Tuple3<Long, String, String>,
						Tuple3<Long, String, String>> chooseSelectedFields = new JoinFunction<Tuple3<Long, String, Integer>, //java 11 可以推断这里的类型，java8会报错
																								Tuple3<Long, String, String>,Tuple3<Long, String, String>>() {
					@Override
					public Tuple3<Long, String, String> join(Tuple3<Long, String, Integer> first,
															 Tuple3<Long, String, String> second) throws Exception {
						//之所以要判空，如果表不是inner join时，有一方会是null
						return new Tuple3<Long, String, String>(
								first==null?null:first.f0, first==null?null:first.f1, second==null?null:second.f2);
					}
				};
				/*
				模拟SQL中的
				select s.id,s.name,a.city from student s left join address a on a.student_id = s.id
				将得到3条结果
				(3,lucy,new york)
				(1,jack,beijing)
				(4,daniel,null)
				 */
				JoinOperator.EquiJoin<Tuple3<Long, String, Integer>,
						Tuple3<Long, String, String>,
						Tuple3<Long, String, String>> withOperator = joinOperator.with(chooseSelectedFields);
				JoinFunctionAssigner<Tuple3<Long, String, Integer>, Tuple3<Long, String, String>> leftOutJoin =
						students.leftOuterJoin(address).where("f0").equalTo(0);
				//其他还有rightOuterJoin\fullOuterJoin
				JoinOperator<Tuple3<Long, String, Integer>,
						Tuple3<Long, String, String>,
						Tuple3<Long, String, String>> leftWithOperator =
						leftOutJoin.with(chooseSelectedFields);
				leftWithOperator.print();
				/*
					如同flatMap与map的区别，1转n与1转1的区别
					flatJoin与join的区别相似
				 */
				FlatJoinFunction<Tuple3<Long, String, Integer>,
						Tuple3<Long, String, String>,
						Tuple3<Long, String, String>> flatJoinFunction = new FlatJoinFunction<Tuple3<Long, String, Integer>,
																								Tuple3<Long, String, String>, Tuple3<Long, String, String>>() {
					@Override
					public void join(Tuple3<Long, String, Integer> first,
									 Tuple3<Long, String, String> second,
									 Collector<Tuple3<Long, String, String>> out) throws Exception {
						Tuple3<Long, String, String> result1 = new Tuple3<>(first.f0, first.f1, second.f2);
						Tuple3<Long, String, String> result2 = new Tuple3<>(first.f0, second.f1, second.f2);
						out.collect(result1);
						out.collect(result2);
					}
				};
				joinOperator.with(flatJoinFunction);
				//提示Flink第二个数据集是小数据库
				students.joinWithTiny(address);
				//提示Flink第二个数据集是大数据集
				students.joinWithHuge(address);

				/*
				使用Join算法提示 JoinHint
				 */
				//将第一个数据集广播出去，转换成HashTable存储，适用于第一个数据集小的情况
				students.join(address, JoinOperatorBase.JoinHint.BROADCAST_HASH_FIRST).where("f0").equalTo(0);
				//将第二个数据集广播出去，转换成HashTable存储，适用于第二个数据集小的情况
				students.join(address, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND).where(0).equalTo("f0");
				//将优化工作交给系统处理，可以不写
				students.join(address, JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES).where("f0").equalTo(0);

				//将两个数据集重新分区，将第一个数据集转换成HashTable存储，适用于第一个数据集比第二个数据集小，但两个数据集都比较大的情况
				students.join(address, JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST).where("f0").equalTo(0);
				//将两个数据集重新分区，并将每个分区排序，适用于两个数据集已经排好顺序的情况
				students.join(address, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(0).equalTo("f0");

				//outJoin中也支持相关JoinHint
				students.leftOuterJoin(address, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND).where("f0").equalTo(0);
				/*
				与Join不同，OuterJoin仅支持部分JoinHint
				leftOuterJoin仅支持4种： OPTIMIZER_CHOOSES、BROADCAST_HASH_SECOND、REPARTITION_HASH_SECOND、REPARTITION_SORT_MERGE
				rightOuterJoin仅支持4种：OPTIMIZER_CHOOSES、BROADCAST_HASH_FIRST、REPARTITION_HASH_FIRST、REPARTITION_SORT_MERGE
				fullOuterJoin仅支持2中：OPTIMIZER_CHOOSES、REPARTITION_SORT_MERGE
				 */
			}
			//## 集合操作
			void operateCollection(){
				/*
				CoGroup的用法：将两个数据集根据相同的Key记录组合在一起，相同Key的记录会存放在一个Group中
				Cross：将两个数据集合并成一个数据集，返回被连接的两个数据集的所有数据行的笛卡尔乘积
				 */
				students.cross(address);
				/*
				union 合并两个数据集，类型相同才能合并）
				rebalance() 对数据集中的数据进行平均分布，使得每个分区上的数据量相同
				Hash-Partition partitionByHash 根据给定的Key进行Hash分区，key相同的数据会放入同一个分区中
				 */
				//根据第一个字段进行hash分区，hash结果相同的会放在一个分区中，执行mapPartition操作处理每个分区的数据
				students.partitionByHash("f0").mapPartition(null);
				/*
				Range-Partition 根据指定Key进行Range分区
				 */
				students.partitionByRange("f0").mapPartition(null);
				//Sort Partition 在本地对Dataset数据集中所有分区根据指定字段进行重排序
				students
						//先使用第二个字段 名字 String 对分区数据进行正排序
						.sortPartition("f1", Order.ASCENDING)
						//再按第一个字段 id Long 对分区内数据进行逆序排序
						.sortPartition(0,Order.DESCENDING)
						.mapPartition(null);//在排序的分区上执行MapPartition转换操作
			}

			//## 排序操作
			void reOrderItems(){
				//普通数据集上获取n条记录
				students.first(3);
				//聚合数据集上返回n条记录
				students.groupBy(0).first(3);
				//Group 排序数据集上返回n条记录
				students.groupBy(0).sortGroup(1,Order.ASCENDING).first(3);
				//从students中找 id 和 age 都最小的字段，如果选择的字段具有多个相同值，则在集合中随机选择一条记录返回
				students.minBy(0,2);
				//按第三个字段 年龄 分组，每组中挑id最小的
				students.groupBy(2).minBy(1);
			}
			public static void main(String[] args) throws Exception{
				tryJoin();
			}
		}

		/*
		# DataSinks数据输出
		Flink抽象出了通用的OutputFormat接口，实现类有 TextOutputFormat、CSVOutputFormat
		Flink内置了常用数据存储介质对应的OutputFormat，如 HadoopOutputFormat\JDBCOutputFormat（DataSourcesAPI中有使用案例） 等
		Flink DataSet API 中的数据输出分为3种类型：
		- 基于文件实现的 Dataset #write()
		- 基于通用存储介质 Dataset #output()
		- 简单的客户端输出 Dataset #print()
		 */
		public static class DataSetDataSinks{
			//获取类根目录的方式，推荐
			static String classRootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
			//getClassLoader拿到的根目录都是/target/classes下，这种临时目录不好用，获取src下的resources直接写就行，不用那么费劲
			static String resourcesPath = "src/main/resources/";
			static String filePath = "file/output_test.txt";
			//static String filePath = DataSetDataSinks.class.getResource("/file/output_test").getPath();
			static DataSource<Tuple3<Long, String, String>> address = batchEnv.<Tuple3<Long, String, String>>fromElements(
					new Tuple3<>(1L, "china", "beijing"),
					new Tuple3<>(2L, "china", "shanghai"),
					new Tuple3<>(3L, "america", "new york")
			);
			static void writeToFile(){
				//address.writeAsText(resourcesPath + filePath, FileSystem.WriteMode.OVERWRITE);
				address.writeAsCsv(resourcesPath + filePath,"\n",",", FileSystem.WriteMode.OVERWRITE);
			}

			public static void main(String[] args) throws Exception{
				writeToFile();
				batchEnv.execute("DataSetDataSinks");
			}
		}

		/*
		# 迭代计算
		Flink中的迭代计算有两种模式：
		全量迭代计算 Bulk Iteration [3308] 使用蒙特卡罗方法（Monte Carlo method）计算圆周率[https://zh.wikipedia.org/wiki/%E8%92%99%E5%9C%B0%E5%8D%A1%E7%BE%85%E6%96%B9%E6%B3%95]
		增量迭代计算 Delt Iteration [3309] 官网示例 Propagate Minimum in Graph [https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/dataset/iterations/]
			这个示例是说，有一个连通图，图的顶点颜色、编号各不相同，每个顶点会向周围传播自己的编号和颜色，每个顶点会将颜色修改为编号比自己小的顶点的颜色
			颜色的传播需要时间，所以需要经过几轮迭代整个连通图的顶点颜色取得一致
			这个算法的应用来自社交分析
		 */
		static class IteratorCalculation{
			static DataSource<Tuple3<Long, String, String>> address = batchEnv.<Tuple3<Long, String, String>>fromElements(
					new Tuple3<>(1L, "china", "beijing"),
					new Tuple3<>(2L, "china", "shanghai"),
					new Tuple3<>(3L, "america", "new york")
			);
			static void bulkIteration() throws Exception{
				Integer iterateCount = 100;
				//初始值为0:Iteration Input
				DataSource<Integer> intDataSource = batchEnv.fromElements(0);
				//迭代数据源迭代次数
				IterativeDataSet<Integer> iterativeDataSet = intDataSource.iterate(iterateCount);
				//迭代一次要进行的操作：Step Function
				MapOperator<Integer, Integer> mapOperator = iterativeDataSet.map(new MapFunction<Integer, Integer>() {
					@Override
					public Integer map(Integer value) throws Exception {
						double x = Math.random();
						double y = Math.random();
						value += x * x + y * y <= 1 ? 1 : 0;
						return value;
					}
				});
				//迭代约束条件
				DataSet<Integer> iterateResult = iterativeDataSet.closeWith(mapOperator);
				MapOperator<Integer, Double> mapOperator2 = iterateResult.map(new MapFunction<Integer, Double>() {
					@Override
					public Double map(Integer value) throws Exception {
						return (value / (double) iterateCount) * 4;
					}
				});
				//dataset api 使用print打印结果后就不能再调execute方法。stream api也遇到过
				//否则报 No new data sinks have been defined since the last execution. The last execution refers to the latest call to 'execute()', 'count()', 'collect()', or 'print()'.
				mapOperator2.print();

				//batchEnv.execute("");
			}
			//todo 待研究 增量迭代缺少案例
			static void deltaIteration() throws Exception{
				int iterativeNum = 100;
				//连通图的顶点
				DataSource<Long> vertex = batchEnv.fromElements(1L, 2L, 3L, 4L, 5L, 6L, 7L);
				//两张连通图的边
				DataSource<Tuple2<Long, Long>> edges = batchEnv.fromElements(
						Tuple2.of(1L, 2L),
						Tuple2.of(2L, 3L),
						Tuple2.of(2L, 4L),
						Tuple2.of(3L, 4L),
						Tuple2.of(5L, 6L),
						Tuple2.of(5L, 7L),
						Tuple2.of(6L, 7L)
				);
				//单向边转双向边
				FlatMapOperator<Tuple2<Long, Long>, Tuple2<Long, Long>> flatMapOperator = edges.flatMap(new FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
					@Override
					public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> collector) throws Exception {
						collector.collect(value);
						collector.collect(Tuple2.of(value.f1, value.f0));
					}
				});
				// 将顶点映射为(v,v)的形式 作为 initialSolutionSet
				MapOperator<Long, Tuple2<Long, Long>> initialSolutionSet = vertex.map(new MapFunction<Long, Tuple2<Long, Long>>() {
					@Override
					public Tuple2<Long, Long> map(Long value) throws Exception {
						return Tuple2.of(value, value);
					}
				});
				//将顶点映射为（v，v）的形式，作为initialWorkset
				MapOperator<Long, Tuple2<Long, Long>> initialWorkSet = vertex.map(new MapFunction<Long, Tuple2<Long, Long>>() {
					@Override
					public Tuple2<Long, Long> map(Long value) throws Exception {
						return Tuple2.of(value, value);
					}
				});
				DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iterative =
						initialSolutionSet.iterateDelta(initialWorkSet, iterativeNum, 0);
				/*
				数据集合边做 join 操作，求出
				1,2,3 三个顶点与 (1,2)(1,3)(2,3)... 6对双向边join，相当于表的连接,使用第一个字段join
				(1,1)				(1,2)
				(2,2)				(1,3)
				(3,3) 	inner join 	(2,3)
									(2,1)
									(3,1)
									(3,2)
				 */
				JoinOperator.EquiJoin<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> changes = iterative.getWorkset()
						.join(edges).where(0).equalTo(0)
						.with(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
							@Override
							public Tuple2<Long, Long> join(Tuple2<Long, Long> first, Tuple2<Long, Long> second) throws Exception {
								return Tuple2.of(second.f1, first.f1);//6个tuple2,内容与edges相同
							}
						}).groupBy(0).aggregate(Aggregations.MIN, 1) //(1,2) (2,1) (3,1)
						//
						.join(iterative.getSolutionSet()).where(0).equalTo(0)
						.with(new FlatJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
							@Override
							public void join(Tuple2<Long, Long> first, Tuple2<Long, Long> second, Collector<Tuple2<Long, Long>> out) throws Exception {
								if (first.f1 < second.f1) {
									out.collect(first);//只留下 (2,1) (3,1)
								}
							}
						});
				DataSet<Tuple2<Long, Long>> result = iterative.closeWith(changes, changes);
				result.print();
			}

			public static void main(String[] args) throws Exception{
				bulkIteration();
			}
		}
		/*
		# 广播变量与分布式缓存
		 */
		static class BroadcastData{
			static void broadcastData(){
				DataSource<Integer> dataSource = batchEnv.fromElements(1, 2, 3);
				//withBroadcastSet方法的 数据集必须在广播前已经创建完毕、广播变量的名称需要在当前应用中唯一
				dataSource.map(null).withBroadcastSet(dataSource,"unique_broadcast_name");
				//使用广播变量：从RuntimeContext中取
				batchEnv.generateSequence(1,10)
						.map(new RichMapFunction<Long, String>() {
							List<Long> broadcastData = null;
							@Override
							public String map(Long value) throws Exception {
								return "对应元素" + broadcastData.get(value.intValue());
							}
							@Override
							public void open(Configuration parameters) throws Exception {
								broadcastData = getRuntimeContext().<Long>getBroadcastVariable("unique_broadcast_name");
							}
						});
			}
			//从文件注册缓存
			static void distributionCache(){
				//缓存名称要在应用内唯一，使用时通过RuntimeContext像获取广播变量那样使用这个唯一名称获取缓存
				batchEnv.registerCachedFile("hdfs:///path/file","unique_distri_cache_name");
				//第三个参数指定 文件可执行
				batchEnv.registerCachedFile("","",true);
				batchEnv.generateSequence(1,10)
						.map(new RichMapFunction<Long, String>() {
							File file = null;
							@Override
							public String map(Long value) throws Exception {
								FileInputStream fileInputStream = new FileInputStream(file);
								return null;
							}
							@Override
							public void open(Configuration parameters) throws Exception {
								file = getRuntimeContext().getDistributedCache().getFile("unique_distri_cache_name");
							}
						});
				//使用完缓存文件后，flink会自动从本地文件系统中删除
			}
		}

		/*
		# 语义注解
		加个注解看不出有啥惊人的效果
		也就ReadFields有点用途，减少不必要的数据传输，提升性能
		注意这里是DataSet API
		 */
		static class SemanticAnnotation{
			/*
			转发字段 - Forwarded Fields 代表数据从Function进入后，对指定为Forwarded的Fields不进行修改
			另外还有 @ForwardedFieldsFirst @ForwardedFieldsSecond
			 */
			//函数注解方式
			//输入的Tuple2的第一个字段f0直接作为输出的Tuple2的第二个字段f1进行返回，所以此时使用ForwardedFields进行提示
			@FunctionAnnotation.ForwardedFields("f0->f1")
			static class MyMapper implements MapFunction<Tuple2<Integer,Double>,Tuple2<Double,Integer>>{
				@Override
				public Tuple2<Double, Integer> map(Tuple2<Integer, Double> value) throws Exception {
					return Tuple2.of(value.f1/2, value.f0);
				}
			}
			//算子参数方式
			static void withForwardedFields(){
				DataSource<Tuple3<Long, String, String>> aSource = batchEnv.fromElements(Tuple3.of(120L, "a", "b"));
				DataSource<Tuple3<Long, Integer, String>> bSource = batchEnv.fromElements(Tuple3.of(130L, 12, "c"));
				aSource.join(bSource).where("f0").equalTo(0)
						.with(new JoinFunction<Tuple3<Long, String, String>, Tuple3<Long, Integer, String>, Tuple2<Long,String>>() {
							@Override
							public Tuple2<Long, String> join(Tuple3<Long, String, String> first, Tuple3<Long, Integer, String> second) throws Exception {
								return null;
							}
						}).withForwardedFieldsSecond("");
			}
			/*
			表达式 f1;f3 表示输入函数的Input对象中，第二个和第四个字段在函数计算过程中产生，其余字段全部按照原来位置进行输出
			 */
			@FunctionAnnotation.NonForwardedFields("_2")
			static class MyMapper2 implements MapFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, Integer>> {
				@Override
				public Tuple3<String, Long, Integer> map(Tuple3<String, Long, Integer> value) throws Exception {
					return Tuple3.of(value.f0, (value.f1 + value.f2)/2, value.f2);
				}
			}
			static void nonForwardedFields(){
			}
			/*
			Read Fields注解 函数会读取哪些字段进行处理
			 */
			@FunctionAnnotation.ReadFields("f0;f2")
			static class MyMapper3 implements MapFunction<Tuple4<Integer,Integer,Double,Integer>,Tuple2<Integer,Double>>{
				@Override
				public Tuple2<Integer, Double> map(Tuple4<Integer, Integer, Double, Integer> value) throws Exception {
					if (value.f0 == 12){
						return Tuple2.of(value.f0,value.f2);
					} else {
						return Tuple2.of(value.f1+10, value.f2);
					}
				}
			}
			/*
			多输入函数 如 JoinFunction CoGroup 等，可以使用 ReadFieldsFirst ReadFieldsSecond 注解
			类似的还有 NonForwardedFieldsFirst NonForwardedFieldsSecond
			 */
			@FunctionAnnotation.ReadFieldsFirst("f0;f1")
			@FunctionAnnotation.ReadFieldsSecond("f0")
			static class MyMapper4 implements JoinFunction<Tuple4<Integer,Integer,Double,Integer>,Tuple2<Integer,Double>,String>{
				@Override
				public String join(Tuple4<Integer, Integer, Double, Integer> first, Tuple2<Integer, Double> second) throws Exception {
					return null;//...
				}
			}
			/*
			一旦使用了Read Fields注解，函数中所有参与计算的字段必须在注解中指明，否则会导致计算函数执行失败
			函数未使用的字段也被Read Fields 注解声明了，不会有任何问题
			 */
		}
		public static void main(String[] args) throws Exception{
			simpleWordCount();
		}
	}

	/*
	关系型编程接口 Table API 及 SQL API
	 */
	static class AbountTableSQLAPI{
		public static void main(String[] args) {
			//如何获取Table Environment
			StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
			StreamTableEnvironment tableStreamEnv = TableEnvironment.getTableEnvironment(streamEnv);
			//批处理应用如何获取Table Environment
			ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
			BatchTableEnvironment tableBatchEnv = TableEnvironment.getTableEnvironment(batchEnv);
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
			public static class CustomFlinkKafkaPartitioner extends FlinkKafkaPartitioner<Tuple3<Long,String,Integer>>{
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
	}

	/*
	获取类路径
	 */
	public static void how2GetClassRootPath() {
		URL resource1 = Thread.currentThread().getContextClassLoader().getResource("//");
		URL resource2 = Thread.currentThread().getContextClassLoader().getResource("");
		URL location = FlinkPrimaryAPI.class.getProtectionDomain().getCodeSource().getLocation();
		//getPath还是getFile无所谓
		String path1 = resource1.getPath();
		System.out.println(path1);
		String file1 = resource2.getFile();
		String path = location.getPath();
		String file = location.getFile();
		//如果要获取resources目录下的路径(file前不能加斜线)
		Thread.currentThread().getContextClassLoader().getResource("file/").getPath();

	}

	public static void main(String[] args) throws Exception {
		how2GetClassRootPath();
		//Configuration configuration = new Configuration();
		////configuration.("",2586);
		//StreamExecutionEnvironment webStreamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		//DataStreamSource<String> socketSource = webStreamEnv.fromElements("12","34");
		//socketSource.addSink(new SinkFunction<String>() {
		//	@Override
		//	public void invoke(String value, Context context) throws Exception {
		//		logger.info("socketSink receive: value = {}, watermark = {}", value, context.currentWatermark());
		//	}
		//});
		//socketSource.print("sink_no_2487");
		//webStreamEnv.execute("aaaa");

	}
}
