package io.moquette.broker;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

/**
 * Utility class to represent a clock that can be moved in time.
 * This is used for tests that needs to verify conditions after a certain amount of time.
 * */
class ForwardableClock extends Clock {

    private Clock currentClock;

    ForwardableClock(Clock clock) {
        this.currentClock = clock;
    }

    void forward(Duration period) {
        currentClock = Clock.offset(currentClock, period);
    }

    @Override
    public ZoneId getZone() {
        return currentClock.getZone();
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return currentClock.withZone(zone);
    }

    @Override
    public Instant instant() {
        return currentClock.instant();
    }
}
