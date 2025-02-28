package io.ipolyzos.triggers.session.threshold;

import io.ipolyzos.triggers.UserEvent;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


public class SessionTimeoutTrigger extends Trigger<UserEvent, TimeWindow> {
    private final long sessionTimeout;
    private final long maxDuration;

    private SessionTimeoutTrigger(long sessionTimeout, long maxDuration) {
        this.sessionTimeout = sessionTimeout;
        this.maxDuration = maxDuration;
    }

    public static SessionTimeoutTrigger of(long sessionTimeout, long maxDuration) {
        return new SessionTimeoutTrigger(sessionTimeout, maxDuration);
    }

    @Override
    public TriggerResult onElement(UserEvent element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        long maxTimeout = window.getStart() + maxDuration;
        ctx.registerProcessingTimeTimer(maxTimeout);
        ctx.registerEventTimeTimer(window.maxTimestamp());
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (time == window.getStart() + maxDuration) {
            System.out.println("Triggering window due to max duration reached.");
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (time == window.maxTimestamp()) {
            System.out.println("Triggering window due to inactivity.");
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteProcessingTimeTimer(window.getStart() + maxDuration);
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
        ctx.registerProcessingTimeTimer(window.getStart() + maxDuration);
        ctx.registerEventTimeTimer(window.maxTimestamp());
    }
}
