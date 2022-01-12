package com.learning;

import com.learning.pojos.PageEvent;
import com.learning.pojos.ViewEvent;

import java.util.Arrays;
import java.util.List;

public class FlinkSourceDataUtils {
    public static List<PageEvent> PAGEEVENTS = Arrays.asList(
            PageEvent.createPageEvent("jack","a01","2022-01-03 12:12:15","buy"),
            PageEvent.createPageEvent("jack","a01","2022-01-03 12:15:15", "view"),
            PageEvent.createPageEvent("lucy","a02","2022-01-03 12:17:00", "scroll")
    );
    public static List<ViewEvent> VIEWEVENTS = Arrays.asList(
            ViewEvent.create("jack","a01","2022-01-03 12:12:15","buy")
    );
}
