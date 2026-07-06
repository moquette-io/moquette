package io.moquette.broker.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ScheduledExpirationService<T extends Expirable> {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledExpirationService.class);

    static final Duration FIRER_TASK_INTERVAL = Duration.ofSeconds(1);
    private final DelayQueue<ExpirableTracker<T>> expiringEntities = new DelayQueue<>();
    private final ScheduledFuture<?> expiredEntityTask;
    private final Clock clock;
    private final Consumer<T> action;
    private final ScheduledExecutorService actionsExecutor;

    private final Map<String, ExpirableTracker<T>> expiringEntitiesCache = new HashMap<>();

    public ScheduledExpirationService(Clock clock, Consumer<T> action) {
        this.clock = clock;
        this.action = action;
        this.actionsExecutor = Executors.newSingleThreadScheduledExecutor();
        this.expiredEntityTask = actionsExecutor.scheduleWithFixedDelay(this::checkExpiredEntities,
            FIRER_TASK_INTERVAL.getSeconds(), FIRER_TASK_INTERVAL.getSeconds(),
            TimeUnit.SECONDS);
    }

    // Package-private for testing. Runs from scheduleWithFixedDelay: if an exception escapes, the
    // executor silently stops rescheduling and NO further expirations ever fire again. Never let one
    // throw out, and never let one failing entity skip the rest of the batch.
    void checkExpiredEntities() {
        try {
            List<ExpirableTracker<T>> expiredEntities = new ArrayList<>();
            int drainedEntities = expiringEntities.drainTo(expiredEntities);
            LOG.debug("Retrieved {} expired entity on {}", drainedEntities, expiringEntities.size());

            for (ExpirableTracker<T> tracker : expiredEntities) {
                try {
                    action.accept(tracker.expirable());
                } catch (Throwable th) {
                    LOG.warn("Expiration action failed for an entity; continuing with the remaining ones", th);
                }
            }
        } catch (Throwable th) {
            // Guarantee the periodic firer keeps running regardless.
            LOG.error("Unexpected error while checking expired entities; the periodic task continues", th);
        }
    }

    public void track(String entityId, T entity) {
        if (!entity.expireAt().isPresent()) {
            throw new RuntimeException("Can't track for expiration an entity without expiry instant, client_id: " + entityId);
        }
        ExpirableTracker<T> entityTracker = new ExpirableTracker<>(entity, clock);
        expiringEntities.add(entityTracker);
        expiringEntitiesCache.put(entityId, entityTracker);
    }

    public boolean untrack(String entityId) {
        ExpirableTracker<T> entityTracker = expiringEntitiesCache.get(entityId);
        if (entityTracker == null) {
            return false; // not found
        }
        return expiringEntities.remove(entityTracker);
    }

    public void shutdown() {
        if (expiredEntityTask.cancel(false)) {
            LOG.info("Successfully cancelled expired entities task");
        } else {
            LOG.warn("Can't cancel the execution of expired entities task, was already cancelled? {}, was done? {}",
                expiredEntityTask.isCancelled(), expiredEntityTask.isDone());
        }
        actionsExecutor.shutdownNow();
    }
}
