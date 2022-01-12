package com.learning.pojos;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Data
public class ViewEvent {
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private String userNo;
    private String pageId;
    private long timestamp;
    private String operationType;

    public static ViewEvent create(String userNo,String pageId,String timestamp,String opType){
        ViewEvent viewEvent = new ViewEvent();
        viewEvent.setUserNo(userNo);
        viewEvent.setPageId(pageId);
        viewEvent.setOperationType(opType);
        long epochSecond = LocalDateTime.parse(timestamp, dateTimeFormatter).atZone(ZoneId.systemDefault()).toInstant().getEpochSecond();
        viewEvent.setTimestamp(epochSecond);
        return viewEvent;
    }
}
