package io.ipolyzos.triggers.session;

import io.ipolyzos.triggers.UserEvent;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class SessionProcessFn extends ProcessWindowFunction<UserEvent, String, String, TimeWindow> {

    @Override
    public void process(String key, Context ctx, Iterable<UserEvent> events, Collector<String> out) {
        TimeWindow window = ctx.window();
        List<String> eventTypes = new ArrayList<>();
        for (UserEvent event : events) {
            eventTypes.add(event.getEventType());
        }
        out.collect("Session for user " + key + " [" + new Timestamp(window.getStart()) + "," + new Timestamp(window.getEnd()) + ") -> Events: " + eventTypes);
    }

}
