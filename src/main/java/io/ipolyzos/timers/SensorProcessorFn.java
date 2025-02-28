package io.ipolyzos.timers;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class SensorProcessorFn extends KeyedProcessFunction<String, SensorReading, String> {

    // State to hold running sum, count, current window end, and inactivity timer timestamp
    private ValueState<Double> sumState;
    private ValueState<Long> countState;
    private ValueState<Long> windowEndState;
    private ValueState<Long> inactivityTimerState;

    // Constants for timer durations
    private static final long WINDOW_DURATION = 60_000L;       // 1 minute window in ms
    private static final long INACTIVITY_THRESHOLD = 10_000L;  // 10 seconds inactivity

    @Override
    public void open(OpenContext openContext) throws Exception {
        // Initialize state descriptors (name, type)
        sumState = getRuntimeContext()
                .getState(
                        new ValueStateDescriptor<>("sum", Types.DOUBLE)
                );

        countState = getRuntimeContext()
                .getState(
                        new ValueStateDescriptor<>("count", Types.LONG)
                );

        windowEndState = getRuntimeContext()
                .getState(
                        new ValueStateDescriptor<>("windowEnd", Types.LONG)
                );

        inactivityTimerState = getRuntimeContext()
                .getState(
                        new ValueStateDescriptor<>("inactivityTimer", Types.LONG)
                );
    }

    @Override
    public void processElement(SensorReading sensorReading,
                               KeyedProcessFunction<String, SensorReading, String>.Context context,
                               Collector<String> collector) throws Exception {
        // Extract the event timestamp of the current reading
        long eventTime = sensorReading.getTimestamp();
        Long currentWindowEnd = windowEndState.value();

        // 1) Event-Time Timer Logic for windowing (1-minute tumbling windows per sensor)
        if (currentWindowEnd == null) {
            // This is the first event for this key or the first event after a window reset
            long windowStart = eventTime - (eventTime % WINDOW_DURATION);
            long windowEnd = windowStart + WINDOW_DURATION;
            // register an event-time timer for end of the window
            context.timerService().registerEventTimeTimer(windowEnd);

            windowEndState.update(windowEnd);
            sumState.update(sensorReading.getTemperature());
            countState.update(1L);
        } else if (eventTime < currentWindowEnd) {
            // Still within the current window
            sumState.update(sumState.value() + sensorReading.getTemperature());
            countState.update(countState.value() + 1);
        } else {
            // The new event belongs to a next window (current window has ended)
            // Emit the result for the current window before resetting
            double sum = sumState.value();
            long count = countState.value();
            double avg = sum / count;
            collector.collect(
                    "Average temperature for sensor " + sensorReading.getSensorId()
                            + " for window ending at " + currentWindowEnd + " = " + avg
            );
            // Clear the old window state and cancel the old timer
            context.timerService().deleteEventTimeTimer(currentWindowEnd);

            sumState.clear();
            countState.clear();
            windowEndState.clear();

            // Start a new window for the incoming event
            long windowStart = eventTime - (eventTime % WINDOW_DURATION);
            long windowEnd = windowStart + WINDOW_DURATION;

            context.timerService().registerEventTimeTimer(windowEnd);
            windowEndState.update(windowEnd);
            sumState.update(sensorReading.getTemperature());
            countState.update(1L);
        }
        // 2) Processing-Time Timer Logic for inactivity alert (10s of no events)
        //      Every time we get an event, schedule a processing-time timer X ms in the future.
        //      If a new event comes before that, cancel the previous timer and schedule a new one.
        Long prevTimerTimestamp = inactivityTimerState.value();
        if (prevTimerTimestamp != null) {
            // Remove the old scheduled timer because we got a new event
            context.timerService().deleteProcessingTimeTimer(prevTimerTimestamp);
        }
        // Register a new processing-time timer for now + threshold
        long newTimerTimestamp = context.timerService().currentProcessingTime() + INACTIVITY_THRESHOLD;
        context.timerService().registerProcessingTimeTimer(newTimerTimestamp);
        // Store the new timer's timestamp in state
        inactivityTimerState.update(newTimerTimestamp);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        // Check the domain of the timer (event-time or processing-time)
        if (ctx.timeDomain() == TimeDomain.EVENT_TIME) {
            // Event-time timer fired (window end reached)
            double sum = sumState.value() != null ? sumState.value() : 0.0;
            long count = countState.value() != null ? countState.value() : 0L;
            if (count > 0) {
                double avg = sum / count;
                out.collect("Average temperature for sensor " + ctx.getCurrentKey() +
                        " for window ending at " + timestamp + " = " + avg);
            }
            // Clear window state after emitting result
            sumState.clear();
            countState.clear();
            windowEndState.clear();
//            Thread.sleep(10000); // Uncomment this line to simulate a slow processing time and show the inactivity alert in action
        } else if (ctx.timeDomain() == TimeDomain.PROCESSING_TIME) {
            // Processing-time timer fired (inactivity threshold passed)
            out.collect("ALERT: Sensor " + ctx.getCurrentKey() + " has been inactive for " + (INACTIVITY_THRESHOLD / 1000) + " seconds");
            // Clear the inactivity timer state (no active timer now for this key)
            inactivityTimerState.clear();
        }
    }
}
