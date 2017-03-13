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

package io.moquette.spi.persistence.lmdb;

import static io.moquette.BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME;

import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.lmdbjava.Env;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.moquette.BrokerConstants;
import io.moquette.server.config.IConfig;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.ISessionsStore;
import io.moquette.spi.persistence.IPersistentStore;

/**
 * LMDB main persistence implementation
 * 
 * @author Jan Zajic
 */
public class LMDBPersistentStore implements IPersistentStore {

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

    private static final Logger LOG = LoggerFactory.getLogger(LMDBPersistentStore.class);

    private Env<ByteBuffer> env;
    private final String m_storePath;

    private LMDBMessagesStore m_messageStore;
    private LMDBSessionsStore m_sessionsStore;

    public LMDBPersistentStore(IConfig props) {
        this.m_storePath = props.getProperty(PERSISTENT_STORE_PROPERTY_NAME, BrokerConstants.DEFAULT_LMDB_STORE_PATH);
    }

    /**
     * Factory method to create message store backed by LMDB
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
        LOG.info("Initializing LMDB store...");
        if (m_storePath == null || m_storePath.isEmpty()) {
            LOG.warn("The LMDB store file path is empty. We don't support it.");
            throw new IllegalStateException("The LMDB store file path is empty. We don't support it.");
        } else {
            File tmpFile;
            
            LOG.info("Using user-defined LMDB store file. Path = {}.", m_storePath);
            tmpFile = new File(m_storePath);
            boolean folderNewlyCreated = false;
            
            if(!tmpFile.exists()) {
                folderNewlyCreated = tmpFile.mkdirs();
                if(!folderNewlyCreated) {
                    LOG.error(
                            "Unable to open LMDB store dir. Path = {}",
                            m_storePath);
                    throw new RuntimeException(
                            "Can't create temp file for subscriptions storage [" + m_storePath + "]");
                }
            }
            
            LOG.warn("Using {} LMDB store dir. Path = {}.", folderNewlyCreated ? "fresh" : "existing", m_storePath);

            env = Env.create()
                    // LMDB also needs to know how large our DB might be. Over-estimating is OK.
                    .setMapSize(10_485_760)
                    // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
                    .setMaxDbs(LMDBMessagesStore.DBS_COUNT + LMDBSessionsStore.DBS_COUNT)
                    // Now let's open the Env. The same path can be concurrently opened and
                    // used in different processes, but do not open the same path twice in
                    // the same process at the same time.
            .open(tmpFile);
        }
        
        // TODO check m_db is valid and
        m_messageStore = new LMDBMessagesStore(env);
        m_messageStore.initStore();

        m_sessionsStore = new LMDBSessionsStore(env, m_messageStore);
        m_sessionsStore.initStore();
    }

    public void close() {
        if (this.env == null || this.env.isClosed()) {
            LOG.warn("The LMDB store is already closed. Nothing will be done.");
            return;
        }
        LOG.info("Closing LMDB store...");
        this.env.close();
    }
}
