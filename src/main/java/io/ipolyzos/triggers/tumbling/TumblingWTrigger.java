package io.ipolyzos.triggers.tumbling;

import io.ipolyzos.triggers.UserEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

public class TumblingWTrigger {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        WatermarkStrategy<UserEvent> watermarkStrategy = WatermarkStrategy
                .<UserEvent>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        DataStream<UserEvent> eventStream = environment.fromElements(
                new UserEvent("user_1", "page_view",   1738362000000L),  // Feb 1, 2025, 00:00:00 GMT
                new UserEvent("user_1", "add_to_cart", 1738362001000L),
                new UserEvent("user_1", "page_view",   1738362002000L),
                new UserEvent("user_1", "page_view",   1738362003000L),
                new UserEvent("user_1", "checkout",    1738362004000L),
                new UserEvent("user_1", "page_view",   1738362060000L),  // +60 seconds later
                new UserEvent("user_2", "page_view",   1738362000000L),
                new UserEvent("user_2", "page_view",   1738362005000L),
                new UserEvent("user_2", "page_view",   1738362010000L),
                new UserEvent("user_2", "page_view",   1738362015000L),
                new UserEvent("user_2", "checkout",    1738362020000L)   // 5th event
        ).assignTimestampsAndWatermarks(watermarkStrategy);


        // Key by userId and apply a 1-hour tumbling event-time window
        eventStream
                .keyBy(UserEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Duration.ofHours((1))))
                .trigger(new CustomCountTrigger())                // use our custom trigger
                .process(new CountEventsPerWindowFn())             // custom window function to count events
                .print();

        environment.execute("Tumbling Window with Five-Event Early Trigger");
    }
}
