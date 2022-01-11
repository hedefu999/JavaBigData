package com.learning.pojos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalField;

@Data @NoArgsConstructor
public class PageEvent {
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private String userId;
    private String pageId;
    private long timestamp;
    private String operationType;
    private String userNo;

    public static PageEvent createPageEvent(String userId, String pageId, String time,String operationType){
        PageEvent pageEvent = new PageEvent();
        pageEvent.setUserId(userId);
        pageEvent.setPageId(pageId);
        pageEvent.setOperationType(operationType);
        long epochSecond = LocalDateTime.parse(time, dateTimeFormatter).atZone(ZoneId.systemDefault()).toInstant().getEpochSecond();
        pageEvent.setTimestamp(epochSecond);
        return pageEvent;
    }
}
