package com.learning;

import com.learning.pojos.MarsMobilePage4AScore;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Arrays;
import java.util.List;

public class FlinkPractice {
    static Logger logger = LoggerFactory.getLogger("StreamingJob");
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

    public static void main(String[] args) throws Exception{
        Path path = new Path(csvResource.getFile());
        DataStreamSource<Row> streamSource = getRowCsvInputFormat(path, FileProcessingMode.PROCESS_ONCE);
        SingleOutputStreamOperator<MarsMobilePage4AScore> dtoOperator = streamSource.map(input -> {
            String pageType = input.<String>getFieldAs(0);
            Long pageId = input.<Long>getFieldAs(1);
            Long userId = input.<Long>getFieldAs(2);
            String startTime = input.<String>getFieldAs(3);
            return MarsMobilePage4AScore.create(pageType, pageId, userId, startTime);
        }).uid("whatthefuck1");

        KeyedStream<MarsMobilePage4AScore, Long> keyedStream = dtoOperator.filter(new FilterFunction<MarsMobilePage4AScore>() {
            @Override
            public boolean filter(MarsMobilePage4AScore value) throws Exception {
                return true;
            }
        }).keyBy(item -> item.getVipruid());
        //keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(10))).reduce(new AggregationFunction<MarsMobilePage4AScore>() {
        //
        //    @Override
        //    public MarsMobilePage4AScore reduce(MarsMobilePage4AScore value1, MarsMobilePage4AScore value2) throws Exception {
        //        return null;
        //    }
        //});


        dtoOperator.addSink(new PrintSinkFunction<>("一只松鼠", true));


        streamEnv.execute("ascores");
    }
}
