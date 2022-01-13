package com.learning;

import akka.dispatch.ExecutionContexts;
import com.learning.pojos.PageEvent;
import com.learning.pojos.Person;
import com.learning.pojos.ViewEvent;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
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
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.DeltaEvictor;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.ExecutionContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.channels.SelectionKey;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

	/**
	 * 终极特性： # window窗口计算
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
					"current_partition_starttime", (ReduceFunction<Long>) (v1, v2) -> Math.min(v1,v2), Types.LONG);

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

		//通过valueState计算最小值
		static void valueState(){
			pageEventDataSource.keyBy(PageEvent::getUserId)
					.flatMap(new RichFlatMapFunction<PageEvent, Integer>() {
						@Override
						public void flatMap(PageEvent value, Collector<Integer> out) throws Exception {

						}
					});
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
