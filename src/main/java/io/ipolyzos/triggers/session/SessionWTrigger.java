package io.ipolyzos.triggers.session;

import io.ipolyzos.triggers.UserEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;

import java.time.Duration;

public class SessionWTrigger {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        WatermarkStrategy<UserEvent> wmStrategy = WatermarkStrategy
                .<UserEvent>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        DataStream<UserEvent> eventStream = environment.fromElements(
                new UserEvent("user_1", "login",   1738362000000L),             // Feb 1, 2025, 00:20:00 GMT
                new UserEvent("user_1", "click",   1738362005000L),             // Feb 1, 2025, 00:20:05 GMT
                new UserEvent("user_1", "click",   1738362010000L),             // Feb 1, 2025, 00:20:10 GMT
                new UserEvent("user_1", "LOGOUT",  1738362020000L),             // Feb 1, 2025, 00:20:20 GMT
                new UserEvent("user_1", "login",   1738365600000L),             // Feb 1, 2025, 01:20:00 GMT
                new UserEvent("user_1", "click",   1738365605000L),             // Feb 1, 2025, 01:20:05 GMT
                new UserEvent("user_1", "click",   1738365625000L),             // Feb 1, 2025, 01:20:25 GMT
                new UserEvent("user_2", "login",   1738369895000L),             // Feb 1, 2025, 02:31:35 GMT
                new UserEvent("user_2", "click",   1738369899000L),             // Feb 1, 2025, 02:31:39 GMT
                new UserEvent("user_2", "CHECKOUT_COMPLETE",   1738369999000L)  // Feb 1, 2025, 02:33:19 GMT
        ).assignTimestampsAndWatermarks(wmStrategy);

        eventStream
                .keyBy(UserEvent::getUserId)
                .window(EventTimeSessionWindows.withGap(Duration.ofMinutes(15)))  // 15 min inactivity gap
                .trigger(new SessionEndTrigger())
                .process(new SessionProcessFn() )
                .print();

        environment.execute("Session Window with Custom Trigger");
    }
}
