package io.moquette.broker.scheduler;

import java.time.Instant;
import java.util.Optional;

public interface Expirable {
    Optional<Instant> expireAt();
}
