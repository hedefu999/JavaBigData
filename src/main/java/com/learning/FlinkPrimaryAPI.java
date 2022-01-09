package com.learning;

import com.learning.pojos.PageEvent;
import com.learning.pojos.Person;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.channels.SelectionKey;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class FlinkPrimaryAPI {
	static Logger logger = LoggerFactory.getLogger("StreamingJob");
	static StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

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
			}).returns(new TypeHint<Integer>() {});//通过returns方法指定返回类型参数？？？不知啥用途
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
	 * 研究下大名鼎鼎的 # DataStream API
	 */
	static class DataStreamAPI{
		//## 内置数据源
		static class InnerDataSource{
			//### 文本数据源(待调试)
			static void fileBasedInput(){
				//1. 简单地从纯文本文件读取
				//DataStreamSource<String> source = streamEnv.readTextFile("");
				//2. 从csv文件读取内容
				String csvFilePath = "/Users/hedefu/Documents/Developer/IDEA/JavaStack/JavaBigData/src/main/resources/file/testcsv.csv";
				Path path = new Path(csvFilePath);
				CsvInputFormat<String> csvInputFormat = new CsvInputFormat<String>(path) {
					@Override
					protected String fillRecord(String s, Object[] objects) {
						logger.info("fileRecord: s = {}, objects = {}",s,objects.length);
						return null;
					}
				};
				//DataStreamSource<String> source1 = streamEnv.readFile(csvInputFormat, csvFilePath);
				DataStreamSource<String> csvSource = streamEnv.readFile(csvInputFormat, csvFilePath,
						FileProcessingMode.PROCESS_ONCE,1000/*ms*/);
				/*
				WatchType分为:PROCESS_CONTINUOUSLY - 一旦检测到文件发生变化，Flink会将该文件全部内容加载到Flink系统中；
							PROCESS_ONCE - 只会将变化的内容送入Flink，可以保证数据的Exactly Once特性
				 */
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
	}

	/**
	 * 更出名的：# 时间概念与Watermark
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
			List<PageEvent> input = Arrays.asList(
					PageEvent.createPageEvent("jack","a01","2022-01-03 12:12:15","buy"),
					PageEvent.createPageEvent("jack","a01","2022-01-03 12:15:15", "view"),
					PageEvent.createPageEvent("lucy","a02","2022-01-03 12:17:00", "scroll")
			);
			//1. 在Source Function中直接定义Timestamps 和 Watermarks
			streamEnv.addSource(new SourceFunction<PageEvent>() {
				@Override
				public void run(SourceContext<PageEvent> ctx) throws Exception {
					input.forEach(item -> {
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
			DataStreamSource<PageEvent> pageEventDataSource = streamEnv.fromCollection(input);
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
					return lastElement.getUserId().contains("黑名单")?new Watermark(extractedTimestamp):null;
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

	public static void main(String[] args) throws Exception {
		DataStreamSource<String> socketSource = streamEnv.socketTextStream("localhost", 9862,"\n");
		socketSource.addSink(new SinkFunction<String>() {
			@Override
			public void invoke(String value, Context context) throws Exception {
				logger.info("socketSink receive: value = {}, watermark = {}", value, context.currentWatermark());
			}
		});
		socketSource.print("sink_no_2487");
		streamEnv.execute("aaaa");
		//final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//env.readTextFile("file:///file/testdata");
		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * https://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		// execute program
		//env.execute("Flink Streaming Java API Skeleton");
	}
}
