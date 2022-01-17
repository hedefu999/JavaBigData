package com.learning.function;

import com.learning.pojos.AScoreVariablesResult;
import com.learning.pojos.MarsMobilePage4AScore;
import org.apache.commons.compress.utils.Sets;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalField;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class WphAScoreVariablesWindowFunction extends ProcessWindowFunction<MarsMobilePage4AScore, AScoreVariablesResult, Long, TimeWindow> {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());
    public static final HashSet<Long> FAKE_ORDERS_PAGE_IDS = Sets.newHashSet(12L, 26L);
    //有状态计算：准备从RuntimeContext中拿到上下文变量（注意如果MapState#get找不到，返回默认值是null，这个在MapStateDescriptor中写死了）
    static MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<String, Long>("user_statistics", String.class, Long.class);
    //todo 其他 KeyedStateDescriptor 要写一下
    static StateTtlConfig ttlConfig = StateTtlConfig
            //todo 这个有效时间可以自动清空状态变量？？？
            .newBuilder(org.apache.flink.api.common.time.Time.days(1))
            .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();

    static ValueStateDescriptor<HashSet<String>> pageTypeValueStateDescriptor =
            new ValueStateDescriptor<HashSet<String>>("page_type"+"20210101", TypeInformation.<HashSet<String>>of(new TypeHint<HashSet<String>>(){}));
    static ValueStateDescriptor<List<Long>> pageIdValueStateDescriptor =
            new ValueStateDescriptor<List<Long>>("page_id"+"20210101", TypeInformation.of(new TypeHint<List<Long>>() {}));
    static {
        descriptor.enableTimeToLive(ttlConfig);
        pageIdValueStateDescriptor.enableTimeToLive(ttlConfig);
        pageTypeValueStateDescriptor.enableTimeToLive(ttlConfig);
    }
    /**
     MapState中保存的变量：
     - 用户访问过的 pageType\pageId
     */
    private MapState<String, Long> statistics;
    private ListState<String> pageTypeListState;
    //当天浏览过的pageType状态变量
    private ListState<String> pageTypesState;
    private ValueState<HashSet<String>> pageTypeValueState;
    private ValueState<List<Long>> pageIdValueState;
    @Override
    public void process(Long key, Context context, Iterable<MarsMobilePage4AScore> elements, Collector<AScoreVariablesResult> out) throws Exception {
        //能不能在重复出发的情况下保证计算结果

        AScoreVariablesResult statistics = new AScoreVariablesResult();
        statistics.setUserId(key);

        //使用SessionWindow计算简版的page_on_time
        List<MarsMobilePage4AScore> sortedElements = StreamSupport.stream(elements.spliterator(), false).sorted().collect(Collectors.toList());
        HashSet<String> pageTypes = pageTypeValueState.value();
        //
        HashSet<String> newPageTypes = new HashSet<>();
        int size = sortedElements.size();
        if (size == 0) {
            out.collect(statistics);
            return;
        }
        for (int i = 0; i < size; i++) {
            MarsMobilePage4AScore item = sortedElements.get(i);
            //1. 这个变量很简单那
            statistics.pageOnTimeCnt ++;
            //2. 加个条件
            if ("page_commodity_detail".equals(item.getPageType())){
                statistics.spxqyPageOnCnt ++;
            }
            //3. 要使用Keyed State
            if (! pageTypes.contains(item.getPageType()) && ! newPageTypes.contains(item.getPageType())){
                statistics.pageTypeCnt ++;
                //窗口中的元素之间也有可能重复
                newPageTypes.add(item.getPageType());
            }
            //4. 最复杂的，要做两次遍历，先准备数据
            //page_on_time 的计算办法与批处理类似,缺点：一个窗口的最后一个Page日志无法计算页面停留时长 todo check 目前先忽略吧
            if (i != size - 1){
                item.pageOnTime = sortedElements.get(i+1).getPageStartTime() - item.getPageStartTime();
            }
            //todo 真不行拿窗口结尾作为最后一个页面的退出时间 context.window().getEnd() 还是固定90s？？
            //5. 浏览页面不重复id数


        }
        //6. 总浏览时长好算
        statistics.pageOnTimeSum = sortedElements.get(sortedElements.size() - 1).pageStartTime - sortedElements.get(0).pageStartTime;
        for (MarsMobilePage4AScore item : sortedElements){
            //7. 带条件的总浏览时长：订单页面浏览时长
            if ("page_commodity_detail".equals(item.getPageType())){
                statistics.spxqyPageOnTime += item.pageOnTime;
            }
            if (FAKE_ORDERS_PAGE_IDS.contains(item.getPageId())){
                statistics.goodsPageOnTime += item.pageOnTime;
            }
            if (FAKE_ORDERS_PAGE_IDS.contains(item.getPageId())){
                statistics.activityPageOnTime += item.pageOnTime;
            }
            if (FAKE_ORDERS_PAGE_IDS.contains(item.getPageId())){
                statistics.orderPageOnTime += item.pageOnTime;
            }
            if (FAKE_ORDERS_PAGE_IDS.contains(item.getPageId())){ //shoppingcart
                statistics.shopPageOnTime += item.pageOnTime;
            }
            Instant instant = Instant.ofEpochMilli(item.pageStartTime);
            int hour = instant.atZone(ZoneId.systemDefault()).get(ChronoField.HOUR_OF_DAY);
            if (6 <= hour && hour <= 10){
                statistics.hour610PageOnTime += item.pageOnTime;
            }
        }
        //更新pageType集合给同key的下一个窗口使用
        pageTypes.addAll(newPageTypes);
        pageTypeValueState.update(pageTypes);


    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //logger.info("松鼠：open and get stat from context"); //调用16次
        RuntimeContext runtimeContext = getRuntimeContext();
        statistics = runtimeContext.getMapState(descriptor);
    }

    @Override
    public void close() throws Exception {
        //logger.info("松鼠：trigger close method"); //11条数据调用16次
    }

}
