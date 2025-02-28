package io.ipolyzos.timers;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class SensorMonitoring {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(new Configuration());

        // We set a parallelism of 1 for demonstration
        // so that the ordering and timers are easier to follow (all in one thread).
        environment.setParallelism(1);

        DataStream<SensorReading> sensorStream = environment.fromElements(
                new SensorReading("sensor_1", 0L, 22.5),
                new SensorReading("sensor_1", 5000L, 23.0),     // 5 sec later
                new SensorReading("sensor_1", 11000L, 21.0),    // 11 sec (out-of-order relative to next perhaps)
                new SensorReading("sensor_1", 10000L, 24.0),    // 10 sec (slightly out of order)
                new SensorReading("sensor_2", 3000L, 30.0),
                new SensorReading("sensor_2", 61000L, 32.0)     // slightly after one minute
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
        );

        sensorStream
                .keyBy(SensorReading::getSensorId)
                .process(new SensorProcessorFn())
                .print();

        environment.execute("Sensor Timers Example");
    }
}
