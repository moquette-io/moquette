/*
 * Copyright (c) 2012-2017 The original author or authorsgetRockQuestions()
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.moquette.spi.persistence;

import io.moquette.server.config.IConfig;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.ISessionsStore;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import static io.moquette.BrokerConstants.AUTOSAVE_INTERVAL_PROPERTY_NAME;
import static io.moquette.BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME;

/**
 * MapDB main persistence implementation
 */
public class MapDBPersistentStore implements IPersistentStore {

    /**
     * This is a DTO used to persist minimal status (clean session and activation status) of a
     * session.
     */
    public static class PersistentSession implements Serializable {

        private static final long serialVersionUID = 5052054783220481854L;
        public final boolean cleanSession;

        public PersistentSession(boolean cleanSession) {
            this.cleanSession = cleanSession;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(MapDBPersistentStore.class);

    private DB m_db;
    private final String m_storePath;
    private final int m_autosaveInterval; // in seconds

    protected final ScheduledExecutorService m_scheduler = Executors.newScheduledThreadPool(1);
    private MapDBMessagesStore m_messageStore;
    private MapDBSessionsStore m_sessionsStore;

    public MapDBPersistentStore(IConfig props) {
        this.m_storePath = props.getProperty(PERSISTENT_STORE_PROPERTY_NAME, "");
        this.m_autosaveInterval = Integer.parseInt(props.getProperty(AUTOSAVE_INTERVAL_PROPERTY_NAME, "30"));
    }

    /**
     * Factory method to create message store backed by MapDB
     *
     * @return the message store instance.
     */
    public IMessagesStore messagesStore() {
        return m_messageStore;
    }

    public ISessionsStore sessionsStore() {
        return m_sessionsStore;
    }

    public void initStore() {
        LOG.info("Initializing MapDB store...");
        if (m_storePath == null || m_storePath.isEmpty()) {
            LOG.warn("The MapDB store file path is empty. Using in-memory store.");
            m_db = DBMaker.newMemoryDB().make();
        } else {
            File tmpFile;
            try {
                LOG.info("Using user-defined MapDB store file. Path = {}.", m_storePath);
                tmpFile = new File(m_storePath);
                boolean fileNewlyCreated = tmpFile.createNewFile();
                LOG.warn("Using {} MapDB store file. Path = {}.", fileNewlyCreated ? "fresh" : "existing", m_storePath);
            } catch (IOException ex) {
                LOG.error(
                        "Unable to open MapDB store file. Path = {}, cause = {}, errorMessage = {}.",
                        m_storePath,
                        ex.getCause(),
                        ex.getMessage());
                throw new RuntimeException(
                        "Can't create temp file for subscriptions storage [" + m_storePath + "]",
                        ex);
            }
            m_db = DBMaker.newFileDB(tmpFile).make();
        }
        LOG.info("Scheduling MapDB commit task...");
        m_scheduler.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {
                LOG.debug("Committing to MapDB...");
                m_db.commit();
            }
        }, this.m_autosaveInterval, this.m_autosaveInterval, TimeUnit.SECONDS);

        // TODO check m_db is valid and
        m_messageStore = new MapDBMessagesStore(m_db);
        m_messageStore.initStore();

        m_sessionsStore = new MapDBSessionsStore(m_db, m_messageStore);
        m_sessionsStore.initStore();
    }

    public void close() {
        if (this.m_db.isClosed()) {
            LOG.warn("The MapDB store is already closed. Nothing will be done.");
            return;
        }
        LOG.info("Performing last commit to MapDB...");
        this.m_db.commit();
        LOG.info("Closing MapDB store...");
        this.m_db.close();
        LOG.info("Stopping MapDB commit tasks...");
        this.m_scheduler.shutdown();
        try {
            m_scheduler.awaitTermination(10L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
        if (!m_scheduler.isTerminated()) {
            LOG.warn("Forcing shutdown of MapDB commit tasks...");
            m_scheduler.shutdown();
        }
        LOG.info("The MapDB store has been closed successfully.");
    }
}
