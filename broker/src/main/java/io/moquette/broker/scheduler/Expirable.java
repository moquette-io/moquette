package io.moquette.broker.scheduler;

import java.time.Instant;

public interface Expirable {
    Instant expireAt();
}
