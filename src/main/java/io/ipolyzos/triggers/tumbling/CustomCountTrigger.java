package io.ipolyzos.triggers.tumbling;

import io.ipolyzos.triggers.UserEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class CustomCountTrigger extends Trigger<UserEvent, TimeWindow> {
    private static final long serialVersionUID = 1L;

    // ValueState to count elements in the window
    private final ValueStateDescriptor<Integer> countStateDesc =
            new ValueStateDescriptor<>("count", Integer.class);

    @Override
    public TriggerResult onElement(UserEvent userEvent,
                                   long timestamp,
                                   TimeWindow timeWindow,
                                   TriggerContext triggerContext) throws Exception {
        // Get or initialize the current count
        ValueState<Integer> countState = triggerContext.getPartitionedState(countStateDesc);
        Integer count = countState.value();
        if (count == null) {
            count = 0;
        }
        // Increment count for every element
        count += 1;
        countState.update(count);

        // If this is the first element, register an event-time timer for end-of-window
        // (Timers are set at window end timestamp, so when watermark passes window.end, onEventTime will fire)
        if (count == 1) {
            long windowEnd = timeWindow.getEnd();  // end timestamp of this TimeWindow
            triggerContext.registerEventTimeTimer(windowEnd);
        }

        // Check if we've reached 5 events in this window
        if (count >= 5) {
            // Fire (emit the window) *now*, but do NOT purge (we return FIRE, not FIRE_AND_PURGE).
            // This means the window contents remain, and the window will possibly fire again at end-of-window.
            return TriggerResult.FIRE;
        } else {
            // Not yet reached 5, so continue accumulating
            return TriggerResult.CONTINUE;
        }    }

    @Override
    public TriggerResult onProcessingTime(long timestamp, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        // We don't use processing-time timers in this trigger.
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long timestamp, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        // This is called when the event-time timer for the window fires (i.e., watermark reached window end).
        if (timestamp == timeWindow.getEnd()) {
            // Window end reached, so fire the window result.
            // We return FIRE_AND_PURGE to emit the result and clear the window state.
            return TriggerResult.FIRE_AND_PURGE;
        }
        // If it's not the window-end timer (e.g., some other timer), we ignore.
        return TriggerResult.CONTINUE;        }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        // Clean up the count state when the window is purged/closed.
        ValueState<Integer> countState = triggerContext.getPartitionedState(countStateDesc);
        countState.clear();
        // We don't need to manually delete the event-time timer for window end – Flink does that when window closes.
    }
}
