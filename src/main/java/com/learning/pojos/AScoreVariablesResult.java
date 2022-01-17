package com.learning.pojos;

import lombok.Data;

@Data
public class AScoreVariablesResult {
    public long userId;
    //次数累计类
    // 当天浏览页面类型数：当天消息体中 distinct page_type 的总数;
    public long pageTypeCnt;
    //当天浏览次数：同用户每产生一个消息，算做一次浏览（以1ds表中数据量为准）;
    public long pageOnTimeCnt;
    //商详页浏览页面数：累加 page_type='page_commodity_detail' 的消息总数（以1ds表中的数量为准）;
    public long spxqyPageOnCnt;
    //浏览页面大类数
    public long pageNameCnt;

    //累计时长类
    //当天浏览总时长
    public long pageOnTimeSum;
    //当天商详页浏览时长：累加 page_type='page_commodity_detail' 的消息的 page_on_time
    public long spxqyPageOnTime;
    //当天商品页浏览时长：累加 page_id在goods对应的pageIdSet 的消息的 page_on_time;
    public long goodsPageOnTime;
    //当天活动页浏览时长：累加 page_id在activity对应的pageIdSet 的消息的 page_on_time;
    public long activityPageOnTime;
    //订单页当天浏览时长：累加 page_id在order对应的pageIdSet 的消息的 page_on_time;
    public long orderPageOnTime;
    //购物车页当天浏览时长：累加 page_id在shoppingcart对应的pageIdSet 的消息的 page_on_time;
    public long shopPageOnTime;
    //当天在6点到10点的浏览时长: 累加当天消息体中page_start_time在[6,10]的 page_on_time;
    public long hour610PageOnTime;
}
