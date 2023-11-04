package io.moquette.broker.scheduler;

import io.moquette.broker.ISessionsRepository;
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

public class ScheduledExpirationService {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledExpirationService.class);

    static final Duration FIRER_TASK_INTERVAL = Duration.ofSeconds(1);
    private final ScheduledExecutorService delayedWillPublicationsScheduler = Executors.newSingleThreadScheduledExecutor();
    private final DelayQueue<ExpirableTracker<ISessionsRepository.Will>> expiringWills = new DelayQueue();
    private final ScheduledFuture<?> expiredWillsTask;
    private final Clock clock;
    private final Consumer<ISessionsRepository.Will> action;

    public ScheduledExpirationService(Clock clock, Consumer<ISessionsRepository.Will> action) {
        this.clock = clock;
        this.action = action;
        this.expiredWillsTask = delayedWillPublicationsScheduler.scheduleWithFixedDelay(this::checkExpiredWills,
            FIRER_TASK_INTERVAL.getSeconds(), FIRER_TASK_INTERVAL.getSeconds(),
            TimeUnit.SECONDS);
    }

    private void checkExpiredWills() {
        List<ExpirableTracker<ISessionsRepository.Will>> expiredWills = new ArrayList<>();
        int drainedWills = expiringWills.drainTo(expiredWills);
        LOG.debug("Retrieved {} expired will on {}", drainedWills, expiringWills.size());

        expiredWills.stream()
            .map(ExpirableTracker::expirable)
            .forEach(action);
    }

    public ExpirableTracker<ISessionsRepository.Will> track(ISessionsRepository.Will will) {
        ExpirableTracker<ISessionsRepository.Will> willTracker = new ExpirableTracker<>(will, clock);
        expiringWills.add(willTracker);
        return willTracker;
    }

    public boolean untrack(ExpirableTracker<ISessionsRepository.Will> willTracker) {
        return expiringWills.remove(willTracker);
    }

    public void shutdown() {
        if (expiredWillsTask.cancel(false)) {
            LOG.info("Successfully cancelled expired wills task");
        } else {
            LOG.warn("Can't cancel the execution of expired wills task, was already cancelled? {}, was done? {}",
                expiredWillsTask.isCancelled(), expiredWillsTask.isDone());
        }
    }
}
