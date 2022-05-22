package com.bigdata.flink.pojos;

import lombok.Data;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;

@Data
public class MarsMobilePage4AScore implements Comparable<MarsMobilePage4AScore>{
    public static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    /*
    页面埋点编码，用离线名称，解析的为page字段的值,页面类型,可通过关联vipdm.dim_log_app_page_type_config获取页面名称
    (建议使用精简表 vipdm.dim_log_app_page_type https://vipdata.vip.vip.com/new/vipmeta/tableDetail?tableName=vipdm.dim_log_app_page_type&dataType=TABLE&tab=model)
     */
    public String pageType;
    /*
    销售归因需要计算的page_id,页面ID
     */
    public Long pageId;
    //会话ID（SessionID），offline is startup_id,一个访次的唯一标识。打开APP90秒不活动会生成一个新的session
    public String sessionId;
    //用户ID，只要 0-20亿的 keyBy字段
    public Long vipruid;
    //EventTime 用于计算page_on_time >2min为小概率事件
    public Long pageStartTime;

    public long pageOnTime;

    public static MarsMobilePage4AScore create(String pageType, Long pageId, Long vipruid, String startTime){
        MarsMobilePage4AScore score = new MarsMobilePage4AScore();
        score.setPageType(pageType);
        score.setPageId(pageId);
        score.setSessionId(vipruid+startTime);
        score.setVipruid(vipruid);
        long epochSecond = LocalDateTime.parse(startTime, dateTimeFormatter).atZone(ZoneId.systemDefault()).toInstant().getEpochSecond();
        //这个字段被指定为Flink的EventTime，long值必须以毫秒为单位，否则会出现窗口时长指定2秒变成2000秒的奇怪问题
        //而且改对后窗口在批式处理时窗口也正确的变成多个，而且数据也变成有序了！！！相当长时间的本地调试都是基于这个错误的时间戳上，惨
        //设置watermark时注意timestamp字段是毫秒
        score.setPageStartTime(epochSecond*1000);
        return score;
    }

    @Override
    public int compareTo(MarsMobilePage4AScore o) {
        return (int) (this.getPageStartTime() - o.getPageStartTime());
    }

    public static void main(String[] args) {
        LocalDateTime parse = LocalDateTime.parse("20220117062302", dateTimeFormatter);
        System.out.println(parse.getHour());

        //1641823803355
        //long time = new Date().getTime();
        //System.out.println(time);//1642419925990

        long l = LocalDateTime.now().withHour(6).atZone(ZoneOffset.systemDefault()).toInstant().toEpochMilli();
        System.out.println(l);

        Instant instant = Instant.ofEpochMilli(1641823803355L);
        int hour = instant.atZone(ZoneId.systemDefault()).get(ChronoField.HOUR_OF_DAY);
        System.out.println(hour);

        //LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        //int hour = localDateTime.getHour();
        //System.out.println(hour);
    }
}
