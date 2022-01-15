package com.learning.pojos;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Data
public class MarsMobilePage4AScore {
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
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

    public static MarsMobilePage4AScore create(String pageType, Long pageId, Long vipruid, String startTime){
        MarsMobilePage4AScore score = new MarsMobilePage4AScore();
        score.setPageType(pageType);
        score.setPageId(pageId);
        score.setSessionId(vipruid+startTime);
        score.setVipruid(vipruid);
        long epochSecond = LocalDateTime.parse(startTime, dateTimeFormatter).atZone(ZoneId.systemDefault()).toInstant().getEpochSecond();
        score.setPageStartTime(epochSecond);
        return score;
    }
}
