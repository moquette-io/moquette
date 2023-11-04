package io.moquette.broker.scheduler;

import java.time.Clock;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public final class ExpirableTracker<T extends Expirable> implements Delayed {

    private final T expirable;
    private final Clock clock;

    public ExpirableTracker(T expirable, Clock clock) {
        this.expirable = expirable;
        this.clock = clock;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(expirable.expireAt().get().toEpochMilli() - clock.millis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
    }

    public T expirable() {
        return expirable;
    }
}
