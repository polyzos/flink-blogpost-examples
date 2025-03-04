package io.ipolyzos.triggers.session.gap;

import io.ipolyzos.triggers.UserEvent;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Mimics a custom "session" with:
 *  - inactivity gap = 5 seconds
 *  - max session length = 7 seconds
 *
 * We forcibly close the session if the next event arrives after inactivityTimer,
 * or if we exceed sessionStart + maxGap.
 */
public class SessionProcessFunction extends KeyedProcessFunction<String, UserEvent, String> {
    private static final long serialVersionUID = 1L;

    // parameters
    private final long gapMs = 5000L;    // 5s inactivity
    private final long maxGapMs = 7000L; // 7s from first event

    // states
    private ValueState<Long> sessionStart;       // first event's timestamp in this session
    private ValueState<Long> inactivityDeadline; // the next inactivity "fire" timestamp
    private ListState<UserEvent> buffer;         // events in the current session
    private ValueState<Long> maxTimer;           // stores the actual timestamp for max-window timer

    @Override
    public void open(Configuration parameters) {
        sessionStart = getRuntimeContext().getState(
                new ValueStateDescriptor<>("sessionStart", Types.LONG)
        );
        inactivityDeadline = getRuntimeContext().getState(
                new ValueStateDescriptor<>("inactivityDeadline", Types.LONG)
        );
        buffer = getRuntimeContext().getListState(
                new ListStateDescriptor<>("buffer", Types.POJO(UserEvent.class))
        );
        maxTimer = getRuntimeContext().getState(
                new ValueStateDescriptor<>("maxTimer", Types.LONG)
        );
    }

    @Override
    public void processElement(UserEvent event, Context ctx, Collector<String> out) throws Exception {
        long eventTs = event.getTimestamp();
        Long start = sessionStart.value();
        Long inactivityTs = inactivityDeadline.value();

        if (start == null) {
            // No current session => start new
            startNewSession(event, ctx);
        } else {
            // We have an open session. Check if we've already passed inactivity time.
            // i.e. if eventTs >= inactivityTs => we definitely went "gap" ms without an event.
            if (inactivityTs != null && eventTs >= inactivityTs) {
                // That means inactivity triggered. Close the old session, reason = "Inactivity".
                emitWindow(out, "Inactivity");
                clearSession(ctx);

                // start a new session for the new event
                startNewSession(event, ctx);

            } else {
                // not inactive yet => check if we exceed max
                long maxClose = start + maxGapMs;
                if (eventTs > maxClose) {
                    // close old session by "Max"
                    emitWindow(out, "Max");
                    clearSession(ctx);

                    // start new session
                    startNewSession(event, ctx);

                } else {
                    // remain in the same session
                    buffer.add(event);

                    // reset inactivity timer to (thisEvent + gapMs)
                    if (inactivityTs != null) {
                        ctx.timerService().deleteEventTimeTimer(inactivityTs);
                    }
                    long newInactivity = eventTs + gapMs;
                    inactivityDeadline.update(newInactivity);
                    ctx.timerService().registerEventTimeTimer(newInactivity);

                    // the max timer remains the same
                }
            }
        }
    }

    /**
     * Called when a registered event-time timer fires.
     * We only rely on them if no more events arrive at all.
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        Long start = sessionStart.value();
        if (start == null) {
            // No active session
            return;
        }

        Long inact = inactivityDeadline.value();
        Long maxT  = maxTimer.value();

        if (inact != null && timestamp == inact) {
            // inactivity timer => close
            emitWindow(out, "Inactivity");
            clearSession(ctx);
        }
        else if (maxT != null && timestamp == maxT) {
            // max timer => close
            emitWindow(out, "Max");
            clearSession(ctx);
        }
    }

    private void startNewSession(UserEvent event, Context ctx) throws Exception {
        long ts = event.getTimestamp();

        // set start to this event's timestamp
        sessionStart.update(ts);

        // clear old buffer, add this event
        buffer.clear();
        buffer.add(event);

        // schedule inactivity timer at (ts + gapMs)
        long inact = ts + gapMs;
        inactivityDeadline.update(inact);
        ctx.timerService().registerEventTimeTimer(inact);

        // schedule max timer at (sessionStart + maxGapMs)
        long maxClose = ts + maxGapMs;
        maxTimer.update(maxClose);
        ctx.timerService().registerEventTimeTimer(maxClose);
    }

    private void emitWindow(Collector<String> out, String reason) throws Exception {
        Long start = sessionStart.value();
        if (start == null) {
            return;
        }
        // gather timestamps
        List<Long> timestamps = new ArrayList<>();
        for (UserEvent e : buffer.get()) {
            timestamps.add(e.getTimestamp());
        }
        timestamps.sort(Long::compare);

        long windowStart = start;
        long windowEnd;
        if ("Max".equals(reason)) {
            windowEnd = start + maxGapMs;
        } else {
            // "Inactivity"
            // by the time we declare inactivity,
            // it's typically lastEvent + gap (or the timer timestamp).
            long last = timestamps.get(timestamps.size() - 1);
            windowEnd = last + gapMs;
        }

        String outStr = String.format(
                "Window(%s) // closed by %s (start=%d, end=%d)",
                timestamps, reason, windowStart, windowEnd
        );
        out.collect(outStr);
    }

    private void clearSession(Context ctx) throws Exception {
        sessionStart.clear();
        inactivityDeadline.clear();
        maxTimer.clear();
        buffer.clear();
    }
}

