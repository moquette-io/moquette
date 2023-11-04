package io.moquette.broker.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ScheduledExpirationService<T extends Expirable> {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledExpirationService.class);

    static final Duration FIRER_TASK_INTERVAL = Duration.ofSeconds(1);
    private final ScheduledExecutorService delayedWillPublicationsScheduler = Executors.newSingleThreadScheduledExecutor();
    private final DelayQueue<ExpirableTracker<T>> expiringEntities = new DelayQueue();
    private final ScheduledFuture<?> expiredEntityTask;
    private final Clock clock;
    private final Consumer<T> action;

    public ScheduledExpirationService(Clock clock, Consumer<T> action) {
        this.clock = clock;
        this.action = action;
        this.expiredEntityTask = delayedWillPublicationsScheduler.scheduleWithFixedDelay(this::checkExpiredEntities,
            FIRER_TASK_INTERVAL.getSeconds(), FIRER_TASK_INTERVAL.getSeconds(),
            TimeUnit.SECONDS);
    }

    private void checkExpiredEntities() {
        List<ExpirableTracker<T>> expiredEntities = new ArrayList<>();
        int drainedEntities = expiringEntities.drainTo(expiredEntities);
        LOG.debug("Retrieved {} expired entity on {}", drainedEntities, expiringEntities.size());

        expiredEntities.stream()
            .map(ExpirableTracker::expirable)
            .forEach(action);
    }

    public ExpirableTracker<T> track(T entity) {
        ExpirableTracker<T> entityTracker = new ExpirableTracker<>(entity, clock);
        expiringEntities.add(entityTracker);
        return entityTracker;
    }

    public boolean untrack(ExpirableTracker<T> entityTracker) {
        return expiringEntities.remove(entityTracker);
    }

    public void shutdown() {
        if (expiredEntityTask.cancel(false)) {
            LOG.info("Successfully cancelled expired entities task");
        } else {
            LOG.warn("Can't cancel the execution of expired entities task, was already cancelled? {}, was done? {}",
                expiredEntityTask.isCancelled(), expiredEntityTask.isDone());
        }
    }
}
