package io.ipolyzos.triggers.session.gap;

import io.ipolyzos.triggers.UserEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class SessionInactivityRunner {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<UserEvent> eventStream = environment.fromElements(
                new UserEvent("user1", "click", 1000L),
                new UserEvent("user1", "scroll", 2000L),
                new UserEvent("user1", "view", 3000L),
                new UserEvent("user1", "click", 5000L),
                new UserEvent("user1", "scroll", 6000L),
                new UserEvent("user1", "click", 9000L),
                new UserEvent("user1", "view", 15000L),
                new UserEvent("user1", "click", 19000L),
                new UserEvent("user1", "click", 22000L)
//                new UserEvent("user1", "click", 1738365600000L), // Feb 1, 2025 00:00:00
//                new UserEvent("user1", "scroll", 1738365605000L), // Feb 1, 2025 00:00:05
//                new UserEvent("user1", "view", 1738365610000L), // Feb 1, 2025 00:00:10
//                new UserEvent("user1", "click", 1738365620000L), // Feb 1, 2025 00:00:20
//                new UserEvent("user1", "scroll", 1738365625000L), // Feb 1, 2025 00:00:25
//                new UserEvent("user1", "click", 1738365630000L), // Feb 1, 2025 00:00:30
//                new UserEvent("user1", "view", 1738365655000L), // Feb 1, 2025 00:00:55
//                new UserEvent("user1", "click", 1738365660000L), // Feb 1, 2025 00:01:00
//                new UserEvent("user1", "click", 1738365665000L)  // Feb 1, 2025 00:01:05
        )  .assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<UserEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp())
        );

        eventStream
                .keyBy(UserEvent::getUserId)
                .process(new SessionProcessFunction())
                .print();

        environment.execute("Custom Session Inactivity/Max Demo");
    }
}
