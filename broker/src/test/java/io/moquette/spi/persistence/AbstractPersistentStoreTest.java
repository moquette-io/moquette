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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.moquette.BrokerConstants;
import io.moquette.server.IntegrationUtils;
import io.moquette.spi.ClientSession;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.ISessionsStore;
import io.moquette.spi.ISessionsStore.ClientTopicCouple;
import io.moquette.spi.MessageGUID;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import io.netty.handler.codec.mqtt.MqttQoS;

/**
 *
 * @author andrea
 */
public abstract class AbstractPersistentStoreTest {

    public static final String TEST_CLIENT = "TestClient";
    
    IPersistentStore m_storageService;
    ISessionsStore m_sessionsStore;
    IMessagesStore m_messagesStore;

    @Before
    public void setUp() throws Exception {
        m_storageService = setUpStore();
        m_storageService.initStore();
        m_messagesStore = m_storageService.messagesStore();
        m_sessionsStore = m_storageService.sessionsStore();
    }

    @After
    public void tearDown() throws Exception {
        if (m_storageService != null) {
            m_storageService.close();
        }
        cleanUpStore();
    }

    protected abstract IPersistentStore setUpStore() throws Exception;
    protected abstract void cleanUpStore() throws Exception;
    
    @Test
    public void overridingSubscriptions() {
        ClientSession session1 = m_sessionsStore.createNewSession("SESSION_ID_1", true);

        // Subscribe on /topic with QOSType.MOST_ONE
        Subscription oldSubscription = new Subscription(session1.clientID, new Topic("/topic"), MqttQoS.AT_MOST_ONCE);
        session1.subscribe(oldSubscription);

        // Subscribe on /topic again that overrides the previous subscription.
        Subscription overridingSubscription = new Subscription(
                session1.clientID,
                new Topic("/topic"),
                MqttQoS.EXACTLY_ONCE);
        session1.subscribe(overridingSubscription);

        // Verify
        List<ClientTopicCouple> subscriptions = m_sessionsStore.listAllSubscriptions();
        assertEquals(1, subscriptions.size());
        Subscription sub = m_sessionsStore.getSubscription(subscriptions.get(0));
        assertEquals(overridingSubscription.getRequestedQos(), sub.getRequestedQos());
    }

    @Test
    public void testNextPacketID_notExistingClientSession() {
        int packetId = m_sessionsStore.nextPacketID("NOT_EXISTING_CLI");
        assertEquals(1, packetId);
    }

    @Test
    public void testNextPacketID_existingClientSession() {
        // Force creation of inflight map for the CLIENT session
        int packetId = m_sessionsStore.nextPacketID("CLIENT");
        assertEquals(1, packetId);

        // request a second packetID
        packetId = m_sessionsStore.nextPacketID("CLIENT");
        assertEquals(2, packetId);
    }

    @Test
    public void testNextPacketID() {
        // request a first ID

        int packetId = m_sessionsStore.nextPacketID("CLIENT");
        m_sessionsStore.inFlight("CLIENT", packetId, new MessageGUID("ABCDE")); // simulate an
                                                                                // inflight
        assertEquals(1, packetId);

        // release the ID
        m_sessionsStore.inFlightAck("CLIENT", packetId);

        // request a second packetID, counter restarts from 0
        packetId = m_sessionsStore.nextPacketID("CLIENT");
        assertEquals(1, packetId);
    }

    @Test
    public void testDropMessagesInSessionCleanAllNotRetainedStoredMessages() {
        m_sessionsStore.createNewSession("TestClient", true);
        IMessagesStore.StoredMessage publishToStore = new IMessagesStore.StoredMessage(
                "Hello".getBytes(),
                MqttQoS.EXACTLY_ONCE,
                "/topic");
        publishToStore.setClientID(TEST_CLIENT);
        publishToStore.setMessageID(1);
        publishToStore.setRetained(false);
        MessageGUID guid = m_messagesStore.storePublishForFuture(publishToStore);

        // Exercise
        m_messagesStore.dropMessagesInSession("TestClient");

        // Verify the message store for session is empty.
        IMessagesStore.StoredMessage storedPublish = m_messagesStore.getMessageByGuid(guid);
        assertNull("The stored message must'n be present anymore", storedPublish);
    }

    @Test
    public void testDropMessagesInSessionDoesntCleanAnyRetainedStoredMessages() {
        m_sessionsStore.createNewSession("TestClient", true);
        IMessagesStore.StoredMessage publishToStore = new IMessagesStore.StoredMessage(
                "Hello".getBytes(),
                MqttQoS.EXACTLY_ONCE,
                "/topic");
        publishToStore.setClientID(TEST_CLIENT);
        publishToStore.setMessageID(1);
        publishToStore.setRetained(true);
        MessageGUID guid = m_messagesStore.storePublishForFuture(publishToStore);

        // Exercise
        m_messagesStore.dropMessagesInSession("TestClient");

        // Verify the message store for session is empty.
        IMessagesStore.StoredMessage storedPublish = m_messagesStore.getMessageByGuid(guid);
        assertNotNull("The stored retained message must be present after client's session drop", storedPublish);
    }
}
