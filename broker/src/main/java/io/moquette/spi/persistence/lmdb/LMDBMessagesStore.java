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
import java.util.UUID;

import org.lmdbjava.Cursor;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.CursorIterator.KeyVal;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.GetOp;
import org.lmdbjava.PutFlags;
import org.lmdbjava.SeekOp;
import org.lmdbjava.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.moquette.spi.IMatchingCondition;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.MessageGUID;
import io.moquette.spi.impl.subscriptions.Topic;

/**
 * IMessagesStore implementation backed by LMDB.
 *
 * @author Jan Zajic
 */
class LMDBMessagesStore extends AbstractLMDBStore implements IMessagesStore {

    private static final Logger LOG = LoggerFactory.getLogger(LMDBMessagesStore.class);

    public static final int DBS_COUNT = 4;
    
    // maps clientID -> guid
    private Dbi<ByteBuffer> retainedStore;
    // maps guid to message, it's message store
    private Dbi<ByteBuffer> persistentMessageStore;
    // maps clientID + messageID -> guid
    Dbi<ByteBuffer> pendingMessageStore;
    // map clientID <-> set of currently in pending packet identifiers
    private Dbi<ByteBuffer> pendingIds;
    
    LMDBMessagesStore(Env<ByteBuffer> env) {
        super(env);
    }

    @Override
    public void initStore() {
        LOG.info("Initializing store...");
        // We need a Dbi for each DB. A Dbi roughly equates to a sorted map. The
        // MDB_CREATE flag causes the DB to be created if it doesn't already
        // exist.
        this.retainedStore = env.openDbi("retained", MDB_CREATE);
        this.persistentMessageStore = env.openDbi("persistedMessages", MDB_CREATE);
        this.pendingMessageStore = env.openDbi("pending", MDB_CREATE);
        this.pendingIds = env.openDbi("pendingPacketIDs", MDB_CREATE, MDB_DUPSORT);
    }

    @Override
    public void storeRetained(Topic topic, MessageGUID guid) {
        LOG.debug("Storing retained messages. Topic = {}, guid = {}.", topic, guid);
        // Dbi.put() internally begins and commits a transaction (Txn).
        this.retainedStore.put(createKey(topic.toString()), createValue(guid.stringValue()));
    }

    @Override
    public Collection<StoredMessage> searchMatching(IMatchingCondition condition) {
        LOG.debug("Scanning retained messages...");
        List<StoredMessage> results = new ArrayList<>();
        try (Txn<ByteBuffer> txn = env.txnRead(); CursorIterator<ByteBuffer> it = this.retainedStore.iterate(txn, FORWARD);) {
            for (final KeyVal<ByteBuffer> kv : it.iterable()) {
                String guidStr = decodeString(kv.val());
                String topicStr = decodeString(kv.key());
                if (condition.match(new Topic(topicStr))) {
                    this.persistentMessageStore.get(txn, createKey(guidStr));
                    ByteBuffer val = txn.val();
                    StoredMessage storedMsg = decodeValue(val);
                    results.add(storedMsg);
                }
            }
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("The retained messages have been scanned. MatchingMessages = {}.", results);
        }

        return results;
    }

    @Override
    public MessageGUID storePublishForFuture(StoredMessage storedMessage) {
        assert storedMessage.getClientID() != null : "The message to be persisted must have a valid client ID";
        MessageGUID guid = new MessageGUID(UUID.randomUUID().toString());
        storedMessage.setGuid(guid);
        LOG.debug("Storing publish event. MqttClientId = {}, messageId = {}, guid = {}, topic = {}.",
                storedMessage.getClientID(), storedMessage.getMessageID(), guid, storedMessage.getTopic());

        this.persistentMessageStore.put(createKey(guid.stringValue()), createValue(storedMessage));
        this.pendingMessageStore.put(createClientMessageKey(storedMessage.getClientID(), storedMessage.getMessageID()), createValue(guid.stringValue()));
        this.pendingIds.put(createKey(storedMessage.getClientID()), createValue(storedMessage.getMessageID()));
        return guid;
    }

    @Override
    public void dropMessagesInSession(String clientID) {
        LOG.debug("Dropping stored messages. ClientId = {}.", clientID);
        ByteBuffer clientKey = createKey(clientID);
        try (Txn<ByteBuffer> txn = env.txnWrite(); final Cursor<ByteBuffer> c = this.pendingIds.openCursor(txn);) {            
            boolean found = c.get(clientKey, GetOp.MDB_SET_KEY);
            while(found == true) {
                int messageID = decodeInt(c.val());
                ByteBuffer clientMessageKey = createClientMessageKey(clientID, messageID);
                this.pendingMessageStore.get(txn, clientMessageKey);
                String guidStr = decodeString(txn.val());
                MessageGUID guid = new MessageGUID(guidStr);
                ByteBuffer messageKey = createKey(guid.stringValue());
                this.persistentMessageStore.get(txn, messageKey);
                final ByteBuffer fetchedVal = txn.val();
                StoredMessage storedMessage = decodeValue(fetchedVal);
                if (!storedMessage.isRetained())
                    this.persistentMessageStore.delete(txn, messageKey);
                this.pendingMessageStore.delete(txn, clientMessageKey);
                found = c.seek(SeekOp. MDB_NEXT_DUP);
            }
            //remove all pendingIds
            c.delete(PutFlags.MDB_NODUPDATA);
            c.close();
            txn.commit();
        }        
    }

    void removeStoredMessage(MessageGUID guid) {
        // remove only the not retained and no more referenced
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            ByteBuffer key = createKey(guid.stringValue());
            this.persistentMessageStore.get(txn, key);
            final ByteBuffer fetchedVal = txn.val();
            StoredMessage storedMessage = decodeValue(fetchedVal);
            if (!storedMessage.isRetained()) {
                LOG.debug("Dropping stored message. ClientId = {}, messageId = {}, guid = {}, topic = {}.",
                        storedMessage.getClientID(), storedMessage.getMessageID(), guid, storedMessage.getTopic());
                this.persistentMessageStore.delete(txn, key);
            }
            // An explicit commit is required, otherwise Txn.close() rolls it back.
            txn.commit();
        }
    }

    @Override
    public StoredMessage getMessageByGuid(MessageGUID guid) {
        LOG.debug("Retrieving stored message. Guid = {}.", guid);
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer key = createKey(guid.stringValue());
            ByteBuffer data = this.persistentMessageStore.get(txn, key);
            if(data != null) {
                final ByteBuffer fetchedVal = txn.val();
                StoredMessage value = decodeValue(fetchedVal);
                return value;
            } else {
                return null;
            }
        }
    }

    @Override
    public void cleanRetained(Topic topic) {
        LOG.debug("Cleaning retained messages. Topic = {}.", topic);
        this.retainedStore.delete(createKey(topic.toString()));
    }

    @Override
    public int getPendingPublishMessages(String clientID) {
        ByteBuffer clientKey = createKey(clientID);
        try (Txn<ByteBuffer> txn = env.txnRead(); final Cursor<ByteBuffer> c = this.pendingIds.openCursor(txn);) {            
            boolean found = c.get(clientKey, GetOp.MDB_SET_KEY);
            if(found)
                return (int) c.count();
            else
                return 0;
        }
    }
    
    @Override
    public MessageGUID mapToGuid(String clientID, int messageID) {        
        LOG.debug("Mapping message ID to GUID CId={}, messageId={}", clientID, messageID);
        try (Txn<ByteBuffer> txn = env.txnRead();) {
            ByteBuffer clientMessageKey = createClientMessageKey(clientID, messageID);
            this.pendingMessageStore.get(txn, clientMessageKey);
            ByteBuffer val = txn.val();
            MessageGUID result = new MessageGUID(decodeString(val));
            LOG.debug("Message ID has been mapped to a GUID CId={}, messageId={}, guid={}", clientID, messageID, result);
            return result;
        }        
    }
    
}
