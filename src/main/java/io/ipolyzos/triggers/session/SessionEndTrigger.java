package io.ipolyzos.triggers.session;

import io.ipolyzos.triggers.UserEvent;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class SessionEndTrigger extends Trigger<UserEvent, TimeWindow> {
    private static final long serialVersionUID = 1L;

    @Override
    public TriggerResult onElement(UserEvent userEvent, long timestamp, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        // Register an event-time timer for the end-of-window (session timeout)
        triggerContext.registerEventTimeTimer(timeWindow.getEnd());
        // If this event is a session terminating event, fire and purge the window
        if ("LOGOUT".equals(userEvent.getEventType()) || "CHECKOUT_COMPLETE".equals(userEvent.getEventType())) {
            return TriggerResult.FIRE_AND_PURGE;
        }
        // Otherwise, continue
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long timestamp, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        // not used in this trigger
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long timestamp, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        // round the timestamp to the next millisecond
        if (timestamp + 1 == timeWindow.getEnd()) {
            // Session inactivity timeout reached, fire and purge the window
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
        // If windows merge, register a new timer for the new window end and let the old timers lapse.
        // (Flink will call onEventTime for the exact timestamps that occur; by re-registering the new end we ensure final firing.)
        ctx.registerEventTimeTimer(window.getEnd());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        triggerContext.deleteEventTimeTimer(timeWindow.maxTimestamp());
    }
}
