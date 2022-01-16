package com.learning;

import com.learning.pojos.MarsMobilePage4AScore;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FlinkPractice {
    static Logger logger = LoggerFactory.getLogger("FlinkPractice");
    public static final String timepatrn = "yyyyMMddHHmmss";
    static StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    static URL csvResource = FlinkPractice.class.getResource("/file/mars_mobile_page.csv");
    //运行出错
    static void howToPojoCsvInputFormat(Path path) throws Exception{
        Class<MarsMobilePage4AScore> pojoClass = MarsMobilePage4AScore.class;
        List<PojoField> pojoFields = Arrays.asList( //字段要setAccessible
                new PojoField(pojoClass.getField("pageType"), Types.STRING),
                new PojoField(pojoClass.getField("pageId"), Types.LONG),
                new PojoField(pojoClass.getField("vipruid"), Types.LONG)
                //new PojoField(pojoClass.getField("pageStartTime"), Types.STRING),
                //new PojoField(pojoClass.getField("sessionId"), Types.STRING)
        );
        PojoTypeInfo<MarsMobilePage4AScore> pojoTypeInfo = new PojoTypeInfo<>(pojoClass, pojoFields);
        PojoCsvInputFormat<MarsMobilePage4AScore> pojoCsvInputFormat = new PojoCsvInputFormat<>(path, pojoTypeInfo);
        pojoCsvInputFormat.setSkipFirstLineAsHeader(false);
    }

    //使用 TupleCsvInputFormat 读取CSV文件,似乎这个类不能用于readFile
    //参考资料 https://soonraah.github.io/posts/read-csv-by-flink-datastream-api/
    static void abountTupleCsvInputFormat(Path path){
        //TypeExtractor.createTypeInfo(MarsMobilePage4AScore.class);
        TypeInformation[] typeInfos = {TypeInformation.of(String.class),TypeInformation.of(Long.class),TypeInformation.of(Long.class),TypeInformation.of(String.class)};
        TupleTypeInfo<Tuple4<String, Long, Long, String>> typeInfo =
                new TupleTypeInfo<Tuple4<String,Long,Long,String>>(typeInfos);
        CsvInputFormat<Tuple4<String,Long,Long,String>> csvInputFormat = new TupleCsvInputFormat<Tuple4<String,Long,Long,String>>(path,typeInfo);
        csvInputFormat.setFieldDelimiter(",");
        csvInputFormat.setSkipFirstLineAsHeader(false);
        DataStreamSource<Tuple4<String, Long, Long, String>> input = streamEnv.createInput(csvInputFormat, typeInfo);
        input.map(Tuple4::toString).print();
    }
    //以后还是用原生点的TextFileInput吧,折腾了好久

    //使用 RowCsvInputFormat 读取 CSV 文件
    static DataStreamSource<Row> getRowCsvInputFormat(Path path, FileProcessingMode processMode){
        TypeInformation[] typeInfos = {TypeInformation.of(String.class),TypeInformation.of(Long.class),TypeInformation.of(Long.class),TypeInformation.of(String.class)};
        CsvInputFormat<Row> csvInputFormat = new RowCsvInputFormat(path,typeInfos);
        csvInputFormat.setSkipFirstLineAsHeader(false);
        return streamEnv.readFile(csvInputFormat, path.getPath(), processMode, 9000L);
    }

    static KeyedStream<MarsMobilePage4AScore, Long> getKeyedStream(){
        //streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Path path = new Path(csvResource.getFile());
        DataStreamSource<Row> streamSource = getRowCsvInputFormat(path, FileProcessingMode.PROCESS_ONCE);
        //设置并行度
        //streamSource.setParallelism(3);
        SingleOutputStreamOperator<MarsMobilePage4AScore> dtoOperator = streamSource.map(input -> {
            String pageType = input.<String>getFieldAs(0);
            Long pageId = input.<Long>getFieldAs(1);
            Long userId = input.<Long>getFieldAs(2);
            String startTime = input.<String>getFieldAs(3);
            return MarsMobilePage4AScore.create(pageType, pageId, userId, startTime);
        }).uid("whatthefuck1");

        //新版Flink API 使用的WaterMark API
        WatermarkStrategy<MarsMobilePage4AScore> watermarkStrategy =
                WatermarkStrategy.<MarsMobilePage4AScore>forBoundedOutOfOrderness(Duration.ofSeconds(5)) //乱序数据考虑的最大时长(再晚来就不要了)
                //.withIdleness(Duration.ofSeconds(100)) //流数据不产生进入闲置状态的超时时长，优化性能
                .withTimestampAssigner((event,timestamp) -> event.getPageStartTime()); //使用SerializableTimestampAssigner通过labmda表达式指定时间戳字段，另一个TimestampAssignerSupplier是通过context方式，接入metricbeats可能用的到

        KeyedStream<MarsMobilePage4AScore, Long> keyedStream = dtoOperator
                .filter((FilterFunction<MarsMobilePage4AScore>) value -> true)
                .assignTimestampsAndWatermarks(watermarkStrategy) //使用新版 assignTimestampsAndWatermarks 的API
                .keyBy(MarsMobilePage4AScore::getVipruid);
        return keyedStream;
    }

    // SessionWindow + aggregate
    static DataStream<Tuple3<Long,Long,Long>> aggregateOperator(KeyedStream<MarsMobilePage4AScore, Long> keyedStream){
        SingleOutputStreamOperator<Tuple3<Long,Long, Long>> operator = keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(10)))
                //第一个是元素，第二个是收集器，最后一个是结果
                .aggregate(new AggregateFunction<MarsMobilePage4AScore, Tuple4<Long,Long, Long, Long>, Tuple3<Long,Long, Long>>() {
                    @Override
                    public Tuple4<Long,Long, Long, Long> createAccumulator() {
                        return new Tuple4<Long,Long, Long, Long>(0L, 0L, 0L, 0L);
                    }
                    //Tuple3<符合条件的累计，上一次时间戳，时间戳累计> 数据到达的顺序无法保证，导致page_on_time计算错误，且每次调试时不同
                    @Override
                    public Tuple4<Long,Long, Long, Long> add(MarsMobilePage4AScore value, Tuple4<Long,Long, Long, Long> accumulator) {
                        accumulator.f0 = value.getVipruid();
                        if (value.getPageType().equalsIgnoreCase("page_commodity_detail")) {
                            accumulator.f1++;
                        }
                        long pageOnTime = accumulator.f2 == 0 ? 0 : (value.pageStartTime - accumulator.f2);
                        accumulator.f2 = value.pageStartTime;
                        accumulator.f3 += pageOnTime;
                        return accumulator;
                    }
                    @Override
                    public Tuple3<Long,Long, Long> getResult(Tuple4<Long,Long, Long, Long> accumulator) {
                        return new Tuple3<>(accumulator.f0, accumulator.f1,accumulator.f3);
                    }
                    //merge时如果元素被打乱/元素被分配到不同节点上，累计时间就是重复的！！？
                    @Override//本地调试时这里没有运行到
                    public Tuple4<Long,Long, Long, Long> merge(Tuple4<Long,Long, Long, Long> a, Tuple4<Long,Long, Long, Long> b) {
                        boolean same = a.f0 == b.f0;
                        long count1 = a.f1 + b.f1;
                        long count2 = a.f3 + b.f3;
                        long interval = Math.abs(a.f2 - b.f2);
                        if (interval < 90) {
                            count2 += interval;
                        }
                        long lastTime = Math.max(a.f2, b.f2);
                        return new Tuple4<>(a.f0, count1, lastTime, count2);
                    }
                });
        return operator;
    }

    static SingleOutputStreamOperator<Map<String, Long>> TumblingWindowProcessAllData(KeyedStream<MarsMobilePage4AScore, Long> keyedStream){
        //将2秒内的数据收集起来批量处理似乎是个不错的办法
        SingleOutputStreamOperator<Map<String,Long>> operator = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .process(new ProcessWindowFunction<MarsMobilePage4AScore, Map<String, Long>, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<MarsMobilePage4AScore> elements, Collector<Map<String, Long>> out) throws Exception {
                        elements.forEach(item -> {/*遍历时仍然是乱序的*/});
                        Stream<MarsMobilePage4AScore> lambdaStream = StreamSupport.stream(elements.spliterator(), false);
                        List<MarsMobilePage4AScore> allData =
                                lambdaStream.sorted((o1, o2) -> (int) (o1.getPageStartTime() - o2.getPageStartTime())).collect(Collectors.toList());
                        //logger.info("松鼠说：userId = {}-{}, comme: {}-{}", key, item.vipruid, item.pageStartTime, item.sessionId);
                        allData.forEach(item -> {/*这样再遍历就不会乱序了*/});
                        //记录2s内的最早和最晚的浏览时间，就是总的页面停留时长！但迟到的数据怎么处理？总浏览时长不超过窗口大小来判断？一个窗口只有一个事件怎么处理？
                        long lastTimestamp = allData.get(allData.size()-1).getPageStartTime();
                        Map<String,Long> map = new HashMap<>();
                        map.put("userId", key);
                        map.put("spxqyPageOnCnt",0L);
                        //这样实现的致命问题是窗口之间的数据不能共享，即这是一个无状态的操作，所以应采用有状态计算的思路
                        KeyedStateStore keyedStateStore = context.globalState();
                        out.collect(new HashMap<>());
                    }
                });

        return operator;
    }
    /*-=-=-=-=-= 上述测试基于EventTime指定字段的时间单位为秒错误，许多结论错误 =-=-=-=-=-=-=-=-=*/



    public static void main(String[] args) throws Exception{
        KeyedStream<MarsMobilePage4AScore, Long> keyedStream = getKeyedStream();
        //DataStream<MarsMobilePage4AScore> operator = aggregateOperator(keyedStream);

        //迟到的数据不能丢弃，计算次数要考虑进去，但计算页面停留时长应排除影响
        //为避免数据重复，窗口计算只能使用 Tumbling/Session Window
        //trigger可以指定窗口计算的触发时机:ContinuousEventTimeTrigger
        //evctor 还用不上，filter足够了

        //迟到的数据能进入窗口吗？（可能要在流式数据中测试，批式的看到数据进来了）
        //时间窗口设置大一点能有更多元素进入process方法
        //时间窗口设置8秒，触发器说2秒计算一次，就会导致
        SingleOutputStreamOperator<Object> operator = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(80)))
                //时间窗口写80，不是说EventTime相差不到80秒就能在一个窗口里，80仅是窗口宽度，至于窗口开始时间怎么确定的（从10秒开始还是从0秒开始，待研究）
                //触发器指定的时长指两个EventTime相差超过40秒就会触发窗口计算，
                //观测到如果两个元素被倒序了，即使已经差40秒（-40秒），不会触发窗口计算
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(40)))
                .allowedLateness(Time.seconds(0))
                .process(new ProcessWindowFunction<MarsMobilePage4AScore, Object, Long, TimeWindow>() {
                    @Override //element元素遍历出来，窗口内是乱序的，但窗口之间是有序的
                    public void process(Long key, Context context, Iterable<MarsMobilePage4AScore> elements, Collector<Object> out) throws Exception {
                        if (key == 101){
                            TimeWindow window = context.window();
                            //window的maxTimestamp 不是当前窗口中出现的最大EventTime，而是window.getEnd()-1s
                            logger.info("松鼠：start-{}, window.start = {}. window.maxts = {}",
                                    key,
                                    DateFormatUtils.format(window.getStart(),timepatrn)+","+DateFormatUtils.format(window.getEnd(),timepatrn),
                                    DateFormatUtils.format(window.maxTimestamp(),timepatrn));
                            elements.forEach(item -> {
                                logger.info("松鼠：{} - {}", key, item.getSessionId());
                            });
                            logger.info("松鼠：end - {}", key);
                        }
                    }
                });


        operator.addSink(new PrintSinkFunction<>("一只松鼠", true));



        streamEnv.execute("ascores");
    }
}
