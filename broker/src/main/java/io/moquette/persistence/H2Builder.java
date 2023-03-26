package io.moquette.persistence;

import io.moquette.broker.IQueueRepository;
import io.moquette.broker.IRetainedRepository;
import io.moquette.broker.ISessionsRepository;
import io.moquette.broker.ISubscriptionsRepository;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class H2Builder {

    private static final Logger LOG = LoggerFactory.getLogger(H2Builder.class);

    private final String storePath;
    private final int autosaveInterval; // in seconds
    private final ScheduledExecutorService scheduler;
    private final Clock clock;
    private MVStore mvStore;

    public H2Builder(ScheduledExecutorService scheduler, Path storePath, int autosaveInterval, Clock clock) {
        this.storePath = storePath.resolve("moquette_store.h2").toAbsolutePath().toString();
        this.autosaveInterval = autosaveInterval;
        this.scheduler = scheduler;
        this.clock = clock;
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    public H2Builder initStore() {
        LOG.info("Initializing H2 store to {}", storePath);
        if (storePath == null || storePath.isEmpty()) {
            throw new IllegalArgumentException("H2 store path can't be null or empty");
        }

        if (!Files.exists(Paths.get(storePath))) {
            try {
                Files.createFile(Paths.get(storePath));
            } catch (IOException ex) {
                throw new IllegalArgumentException("Error creating " + storePath + " file", ex);
            }
        }

        mvStore = new MVStore.Builder()
            .fileName(storePath)
            .autoCommitDisabled()
            .open();

        LOG.trace("Scheduling H2 commit task");
        scheduler.scheduleWithFixedDelay(() -> {
            LOG.trace("Committing to H2");
            mvStore.commit();
        }, autosaveInterval, autosaveInterval, TimeUnit.SECONDS);
        return this;
    }

    public ISubscriptionsRepository subscriptionsRepository() {
        return new H2SubscriptionsRepository(mvStore);
    }

    public void closeStore() {
        mvStore.close();
    }

    public IQueueRepository queueRepository() {
        return new H2QueueRepository(mvStore);
    }

    public IRetainedRepository retainedRepository() {
        return new H2RetainedRepository(mvStore);
    }

    public ISessionsRepository sessionsRepository() {
        return new H2SessionsRepository(mvStore, clock);
    }
}
