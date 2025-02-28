package io.ipolyzos.triggers.session.threshold;

import io.ipolyzos.triggers.UserEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class SessionThresholdRunner {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        WatermarkStrategy<UserEvent> watermarkStrategy = WatermarkStrategy
                .<UserEvent>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        DataStream<UserEvent> eventStream = environment.fromElements(
                new UserEvent("user1", "click", 1738365600000L), // Feb 1, 2025 00:00:00
                new UserEvent("user1", "scroll", 1738365605000L), // Feb 1, 2025 00:00:05
                new UserEvent("user1", "view", 1738365610000L), // Feb 1, 2025 00:00:10
                new UserEvent("user1", "click", 1738365620000L), // Feb 1, 2025 00:00:20
                new UserEvent("user2", "scroll", 1738365625000L), // Feb 1, 2025 00:00:25
                new UserEvent("user2", "click", 1738365630000L), // Feb 1, 2025 00:00:30
                new UserEvent("user2", "view", 1738365655000L), // Feb 1, 2025 00:00:55
                new UserEvent("user3", "click", 1738365660000L), // Feb 1, 2025 00:01:00
                new UserEvent("user3", "click", 1738365665000L)  // Feb 1, 2025 00:01:05
        ).assignTimestampsAndWatermarks(watermarkStrategy);


        // Key by userId and apply a 1-hour tumbling event-time window
        eventStream
                .keyBy(UserEvent::getUserId)
                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .trigger(SessionTimeoutTrigger.of(10_000, 15_000)) // 10 sec inactivity, 15 sec max threshold
                .process(new ProcessWindowFunction<UserEvent, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<UserEvent> elements, Collector<String> out) {
                        long windowEnd = context.window().getEnd();
                        int count = 0;
                        for (UserEvent event : elements) {
                            count++;
                        }

                        // Determine if the window fired due to inactivity or max duration
                        String reason = (windowEnd - context.window().getStart() == 15_000) ? "Max Duration Reached" : "Inactivity";

                        // Convert timestamps to readable format
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);
                        String formattedWindowEnd = formatter.format(Instant.ofEpochMilli(windowEnd));

                        out.collect("User: " + key + ", Window End: " + formattedWindowEnd + ", Event Count: " + count + ", Reason: " + reason);
                    }
                })
                .print();

        environment.execute("Tumbling Window with Five-Event Early Trigger");
    }
}
