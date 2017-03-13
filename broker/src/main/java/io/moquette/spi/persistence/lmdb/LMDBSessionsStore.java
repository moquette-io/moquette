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

import static org.lmdbjava.CursorIterator.IteratorType.FORWARD;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

import org.lmdbjava.Cursor;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.CursorIterator.KeyVal;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.GetOp;
import org.lmdbjava.SeekOp;
import org.lmdbjava.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.moquette.spi.ClientSession;
import io.moquette.spi.IMessagesStore.StoredMessage;
import io.moquette.spi.ISessionsStore;
import io.moquette.spi.MessageGUID;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import io.moquette.spi.persistence.lmdb.LMDBPersistentStore.PersistentSession;

/**
 * ISessionsStore implementation backed by LMDB.
 *
 * @author Jan Zajic
 */
class LMDBSessionsStore extends AbstractLMDBStore implements ISessionsStore {

    private static final Logger LOG = LoggerFactory.getLogger(LMDBSessionsStore.class);

    public static final int DBS_COUNT = 7;
    
    // maps clientID+MessageId -> guid
    private Dbi<ByteBuffer> inflightStore;
    // maps clientID+MessageId -> guid
    private Dbi<ByteBuffer> secondPhaseStore;
    
    private Dbi<ByteBuffer> persistentSessions;
    private Dbi<ByteBuffer> subscriptions;
    // map clientID <-> set of currently in flight packet identifiers
    private Dbi<ByteBuffer> inFlightIds;
    // map clientID <-> set of currently second phase packet identifiers
    private Dbi<ByteBuffer> secondPhaseIds;
    
    private Dbi<ByteBuffer> clientQueue;
    
    private final LMDBMessagesStore m_messagesStore;

    LMDBSessionsStore(Env<ByteBuffer> env, LMDBMessagesStore messagesStore) {
        super(env);
        m_messagesStore = messagesStore;
    }

    @Override
    public void initStore() {
        inflightStore = env.openDbi("inflight", MDB_CREATE);
        persistentSessions = env.openDbi("sessions", MDB_CREATE);
        subscriptions = env.openDbi("subscriptions", MDB_CREATE, MDB_DUPSORT);
        inFlightIds = env.openDbi("inflightPacketIDs", MDB_CREATE, MDB_DUPSORT);
        secondPhaseStore = env.openDbi("secondPhase", MDB_CREATE);
        secondPhaseIds = env.openDbi("secondPhaseIDs", MDB_CREATE, MDB_DUPSORT);
        clientQueue = env.openDbi("clientQueue", MDB_CREATE, MDB_DUPSORT);
    }
    
    @Override
    public ClientSession createNewSession(String clientID, boolean cleanSession) {
        try (Txn<ByteBuffer> txn = env.txnWrite();) {
            ByteBuffer key = createKey(clientID);
            ByteBuffer byteBuffer = persistentSessions.get(txn, key);
            if(byteBuffer != null) {
                LOG.error(
                        "Unable to create a new session: the client ID is already in use. CId= {}, cleanSession = {}.",
                        clientID,
                        cleanSession);
                throw new IllegalArgumentException("Can't create a session with the ID of an already existing" + clientID);    
            } else {
                LOG.info("Creating new session. CId= {}, cleanSession = {}.", clientID, cleanSession);
                persistentSessions.put(txn, key, createValue(new PersistentSession(cleanSession)));
                txn.commit();
                return new ClientSession(clientID, m_messagesStore, this, cleanSession);
            }
        }
    }
    
    @Override
    public ClientSession sessionForClient(String clientID) {
        LOG.info("Retrieving session. CId= {}.", clientID);
        try (Txn<ByteBuffer> txn = env.txnRead();) {
            ByteBuffer key = createKey(clientID);
            ByteBuffer byteBuffer = persistentSessions.get(txn, key);
            if(byteBuffer != null) {
                PersistentSession storedSession = decodeValue(byteBuffer);
                return new ClientSession(clientID, m_messagesStore, this, storedSession.cleanSession);
            } else {
                LOG.warn("The session does not exist. CId= {}.", clientID);
                return null;
            }            
        }
    }
    
    @Override
    public void addNewSubscription(Subscription newSubscription) {
        LOG.info(
                "Adding new subscription. CId= {}, topics = {}.",
                newSubscription.getClientId(),
                newSubscription.getTopicFilter());
        final String clientID = newSubscription.getClientId();
        try (Txn<ByteBuffer> txn = env.txnWrite(); final Cursor<ByteBuffer> c = this.subscriptions.openCursor(txn);) {
            ByteBuffer key = createKey(clientID);
            boolean found = c.get(key, GetOp.MDB_SET_KEY);
            while(found) {
                Subscription decodeValue = decodeValue(c.val());
                Topic storedTopicFilter = decodeValue.getTopicFilter();
                if(storedTopicFilter.equals(newSubscription.getTopicFilter())) {
                    c.delete();
                    break;
                }
                found = c.seek(SeekOp. MDB_NEXT_DUP);
            }            
            c.close();
            
            boolean written = subscriptions.put(txn, createKey(clientID), createValue(newSubscription));
            if(!written) {
                throw new IllegalStateException("cannot write subscription "+newSubscription+" to subscriptions db");
            } else {
                txn.commit();
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace(
                        "The subscription has been added. CId= {}, topics = {}.",
                        newSubscription.getClientId(),
                        newSubscription.getTopicFilter());
            }
        }
    }

    @Override
    public void removeSubscription(Topic topicFilter, String clientID) {
        LOG.info("Removing subscription. CId= {}, topics = {}.", clientID, topicFilter);
        try (Txn<ByteBuffer> txn = env.txnWrite(); final Cursor<ByteBuffer> c = this.subscriptions.openCursor(txn);) {
            ByteBuffer key = createKey(clientID);
            boolean found = c.get(key, GetOp.MDB_SET_KEY);
            while(found == true) {
                Subscription decodeValue = decodeValue(c.val());
                Topic storedTopicFilter = decodeValue.getTopicFilter();
                if(storedTopicFilter.equals(topicFilter)) {
                    c.delete();
                    break;
                }
                found = c.seek(SeekOp. MDB_NEXT_DUP);        
            }
            c.close();
            txn.commit();
        }
    }

    @Override
    public void wipeSubscriptions(String clientID) {
        LOG.info("Wiping subscriptions. CId= {}.", clientID);
        this.subscriptions.delete(createKey(clientID));
        if (LOG.isDebugEnabled()) {
            LOG.debug("The subscriptions have been removed. CId= {}.",
                      clientID);
        }
    }

    @Override
    public List<ClientTopicCouple> listAllSubscriptions() {
        LOG.info("Retrieving existing subscriptions...");
        final List<ClientTopicCouple> allSubscriptions = new ArrayList<>();
        try (Txn<ByteBuffer> txn = env.txnRead(); CursorIterator<ByteBuffer> it = this.subscriptions.iterate(txn, FORWARD);) {
            for (final KeyVal<ByteBuffer> kv : it.iterable()) {
                String clientId = decodeString(kv.key());
                Subscription decodeValue = decodeValue(kv.val());
                allSubscriptions.add(new ClientTopicCouple(clientId, decodeValue.getTopicFilter()));                
            }
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("The existing subscriptions have been retrieved. Result = {}.", allSubscriptions);
        }
        return allSubscriptions;
    }

    @Override
    public Subscription getSubscription(ClientTopicCouple couple) {        
        try (Txn<ByteBuffer> txn = env.txnRead(); final Cursor<ByteBuffer> c = this.subscriptions.openCursor(txn);) {
            ByteBuffer key = createKey(couple.clientID);
            boolean found = c.get(key, GetOp.MDB_SET_KEY);
            while(found) {
                Subscription decodeValue = decodeValue(c.val());
                Topic storedTopicFilter = decodeValue.getTopicFilter();
                if(storedTopicFilter.equals(couple.topicFilter)) {
                    return decodeValue;
                }
                found = c.seek(SeekOp. MDB_NEXT_DUP);                
            }
        }
        return null;        
    }

    @Override
    public List<Subscription> getSubscriptions() {
        LOG.info("Retrieving existing subscriptions...");
        final List<Subscription> allSubscriptions = new ArrayList<>();
        try (Txn<ByteBuffer> txn = env.txnRead(); CursorIterator<ByteBuffer> it = this.persistentSessions.iterate(txn, FORWARD);) {
            for (final KeyVal<ByteBuffer> kv : it.iterable()) {
                String clientId = decodeString(kv.key());
                Subscription decodeValue = decodeValue(kv.val());
                allSubscriptions.add(decodeValue);                
            }
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("The existing subscriptions have been retrieved. Result = {}.", allSubscriptions);
        }
        return allSubscriptions;
    }

    @Override
    public boolean contains(String clientID) {
        try (Txn<ByteBuffer> txn = env.txnRead(); final Cursor<ByteBuffer> c = this.subscriptions.openCursor(txn);) {
            this.subscriptions.get(txn, createKey(clientID));
            return c.count() > 0;
        }
    }

    @Override
    public Collection<ClientSession> getAllSessions() {
        Collection<ClientSession> result = new ArrayList<>();
        try (Txn<ByteBuffer> txn = env.txnRead(); CursorIterator<ByteBuffer> it = this.persistentSessions.iterate(txn, FORWARD);) {
            for (final KeyVal<ByteBuffer> kv : it.iterable()) {
                ByteBuffer key = kv.key();
                ByteBuffer val = kv.val();
                PersistentSession storedSession = decodeValue(val);
                result.add(new ClientSession(decodeString(key), m_messagesStore, this, storedSession.cleanSession));
            }            
        }
        return result;
    }

    @Override
    public void updateCleanStatus(String clientID, boolean cleanSession) {
        LOG.info("Updating cleanSession flag. CId= {}, cleanSession = {}.", clientID, cleanSession);
        try (Txn<ByteBuffer> txn = env.txnWrite();) {
            ByteBuffer key = createKey(clientID);
            ByteBuffer byteBuffer = persistentSessions.get(txn, key);
            if(byteBuffer != null) {
                persistentSessions.put(txn, createKey(clientID), createValue(new LMDBPersistentStore.PersistentSession(cleanSession)));
                txn.commit();
            } else {
                LOG.warn("The session does not exist. CId= {}.", clientID);
                throw new IllegalStateException("The session does not exist. CId="+clientID+".");
            }                
        }
    }

    /**
     * Return the next valid packetIdentifier for the given client session.
     */
    @Override
    public int nextPacketID(String clientID) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Generating next packet ID. CId= {}.", clientID);
        }    
        
        int nextPacketId = 0;
        try (Txn<ByteBuffer> txn = env.txnWrite(); final Cursor<ByteBuffer> c = this.inFlightIds.openCursor(txn);) {
            int maxId = 0;
            ByteBuffer key = createKey(clientID);
            boolean found = c.get(key, GetOp.MDB_SET_KEY);            
            if(found) {
                c.seek(SeekOp.MDB_LAST_DUP);
                ByteBuffer lastValBuffer = c.val();
                maxId = lastValBuffer.getInt();
            }
            nextPacketId = (maxId + 1) % 0xFFFF;
            boolean putted = c.put(key, createValue(nextPacketId));
            if(!putted) {
                throw new IllegalStateException("cannot store nextPacketId "+nextPacketId);
            }
            
            c.close();
            txn.commit();
        }
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("The next packet ID has been generated. CId= {}, result = {}.", clientID, nextPacketId);
        }
        
        return nextPacketId;
    }
    
    @Override
    public void inFlightAck(String clientID, int messageID) {
        LOG.debug("Acknowledging inflight message. CId= {}, messageId = {}.", clientID, messageID);        
        // remove from the ids store
        try (Txn<ByteBuffer> txn = env.txnWrite(); final Cursor<ByteBuffer> c = this.inFlightIds.openCursor(txn);) {
            ByteBuffer inflightKey = createClientMessageKey(clientID, messageID);
            boolean deleted = this.inflightStore.delete(txn, inflightKey);
            if(!deleted) {
                LOG.warn("Unable to retrieve inflight message record. CId= {}, messageId = {}.", clientID, messageID);
                return;
            }
            
            ByteBuffer key = createKey(clientID);
            boolean found = c.get(key, GetOp.MDB_SET_KEY);
            while(found == true) {
                ByteBuffer lastValBuffer = c.val();
                int lastId = lastValBuffer.getInt();
                if(lastId == messageID) {
                    c.delete();
                    break;
                }
                found = c.seek(SeekOp. MDB_NEXT_DUP);                
            }
            
            c.close();
            txn.commit();
        }
    }

    @Override
    public void inFlight(String clientID, int messageID, MessageGUID guid) {
        LOG.debug("Storing inflight message. CId= {}, messageId = {}, guid = {}.", clientID, messageID, guid);
        try (Txn<ByteBuffer> txn = env.txnWrite();) {
            ByteBuffer inflightKey = createClientMessageKey(clientID, messageID);
            this.inflightStore.put(txn, inflightKey, createValue(guid.stringValue()));
            txn.commit();
        }        
        LOG.info("storing inflight clientID <{}> messageID {} guid <{}>", clientID, messageID, guid);
    }

    @Override
    public Queue<StoredMessage> queue(String clientID) {
        LOG.info("Queuing pending message. CId= {}, guid = {}.", clientID);
        return new LMDBQueue(clientID, this.clientQueue, this);
    }

    @Override
    public void dropQueue(String clientID) {
        LOG.info("Removing pending messages. CId= {}.", clientID);
        try (Txn<ByteBuffer> txn = env.txnWrite();) {
            this.clientQueue.delete(txn, createKey(clientID));
            txn.commit();
        }
    }

    @Override
    public void moveInFlightToSecondPhaseAckWaiting(String clientID, int messageID) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Moving inflight message to 2nd phase ack state. CId= {}, messageID = {}.", clientID, messageID);
        }
        
        try (Txn<ByteBuffer> txn = env.txnWrite();) {
            ByteBuffer inflightKey = createClientMessageKey(clientID, messageID);
            ByteBuffer byteBuffer = this.inflightStore.get(txn, inflightKey);
            ByteBuffer guid = null;
            
            if(byteBuffer == null) {
                LOG.warn("Unable to retrieve inflight message record. CId= {}, messageId = {}.", clientID, messageID);
                return;
            } else {
                guid = txn.val();
                this.inflightStore.delete(txn, inflightKey);
            }
            
            ByteBuffer clientIdKey = createKey(clientID);
            ByteBuffer messageIDValue = createValue(messageID);
            
            // remove from the ids store
            this.inFlightIds.delete(txn, clientIdKey, messageIDValue);
            //put to second phase store
            secondPhaseStore.put(inflightKey, guid);
            secondPhaseIds.put(clientIdKey, messageIDValue);
            txn.commit();
        }
    }

    @Override
    public MessageGUID secondPhaseAcknowledged(String clientID, int messageID) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing second phase ACK CId={}, messageId={}", clientID, messageID);
        }
        try (Txn<ByteBuffer> txn = env.txnWrite();) {
            ByteBuffer inflightKey = createClientMessageKey(clientID, messageID);
            this.inflightStore.get(txn, inflightKey);
            String guid = decodeString(txn.val());
            this.secondPhaseStore.delete(txn, inflightKey);
            this.secondPhaseIds.delete(txn, createKey(clientID), createValue(messageID));            
            txn.commit();
            return new MessageGUID(guid);
        }
    }

    @Override
    public MessageGUID mapToGuid(String clientID, int messageID) {        
        return m_messagesStore.mapToGuid(clientID, messageID);
    }

    @Override
    public StoredMessage getInflightMessage(String clientID, int messageID) {
        LOG.info("Retrieving inflight message CId={}, messageId={}", clientID, messageID);
        try (Txn<ByteBuffer> txn = env.txnRead();) {
            ByteBuffer inflightKey = createClientMessageKey(clientID, messageID);
            ByteBuffer byteBuffer = this.inflightStore.get(txn, inflightKey);
            if(byteBuffer == null) {
                LOG.warn("The message ID does not have an associated GUID. CId= {}, messageId = {}.", clientID, messageID);
                return null;
            } else {
                String guidStr = decodeString(txn.val());
                MessageGUID guid = new MessageGUID(guidStr); 
                return m_messagesStore.getMessageByGuid(guid);
            }
            
        }
    }

    @Override
    public int getInflightMessagesNo(String clientID) {
        ByteBuffer clientKey = createKey(clientID);
        try (Txn<ByteBuffer> txn = env.txnRead(); final Cursor<ByteBuffer> c = this.inFlightIds.openCursor(txn);) {            
            boolean found = c.get(clientKey, GetOp.MDB_SET_KEY);
            if(found)
                return (int) c.count();
            else
                return 0;
        }
    }

    @Override
    public int getPendingPublishMessagesNo(String clientID) {
        return m_messagesStore.getPendingPublishMessages(clientID);
    }

    @Override
    public int getSecondPhaseAckPendingMessages(String clientID) {
        ByteBuffer clientKey = createKey(clientID);
        try (Txn<ByteBuffer> txn = env.txnRead(); final Cursor<ByteBuffer> c = this.secondPhaseIds.openCursor(txn);) {            
            boolean found = c.get(clientKey, GetOp.MDB_SET_KEY);
            if(found)
                return (int) c.count();
            else
                return 0;
        }
    }
    
}
