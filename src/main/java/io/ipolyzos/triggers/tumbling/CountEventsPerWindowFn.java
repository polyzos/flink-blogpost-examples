package io.ipolyzos.triggers.tumbling;

import io.ipolyzos.triggers.UserEvent;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class CountEventsPerWindowFn extends ProcessWindowFunction<UserEvent, String, String, TimeWindow> {
    @Override
    public void process(String key, Context context, Iterable<UserEvent> elements, Collector<String> out) {
        long windowStart = context.window().getStart();
        long windowEnd = context.window().getEnd();
        int count = 0;
        for (UserEvent e : elements) {
            count++;
        }
        out.collect("User " + key + " had " + count + " events in window [" + new Timestamp(windowStart) + "," + new Timestamp(windowEnd) + ")");
    }
}
