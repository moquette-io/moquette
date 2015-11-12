/*
 * Copyright (c) 2012-2015 The original author or authors
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
package org.eclipse.moquette.spi.impl;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.eclipse.moquette.interception.InterceptHandler;
import org.eclipse.moquette.proto.messages.*;
import org.eclipse.moquette.server.netty.NettyChannel;
import org.eclipse.moquette.spi.IMatchingCondition;
import org.eclipse.moquette.spi.IMessagesStore;
import org.eclipse.moquette.spi.ISessionsStore;
import org.eclipse.moquette.spi.impl.events.PublishEvent;
import org.eclipse.moquette.spi.persistence.MemorySessionStore;
import org.eclipse.moquette.spi.impl.security.PermitAllAuthorizator;
import org.eclipse.moquette.spi.impl.subscriptions.Subscription;
import org.eclipse.moquette.spi.impl.subscriptions.SubscriptionsStore;

import org.eclipse.moquette.proto.messages.AbstractMessage.QOSType;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

/**
 *
 * @author andrea
 */
public class ProtocolProcessorTest {
    final static String FAKE_CLIENT_ID = "FAKE_123";
    final static String FAKE_CLIENT_ID2 = "FAKE_456";
    final static String FAKE_PUBLISHER_ID = "Publisher";
    final static String FAKE_TOPIC = "/news";
    final static String BAD_FORMATTED_TOPIC = "#MQTTClient";
    
    final static String TEST_USER = "fakeuser";
    final static byte[] TEST_PWD = "fakepwd".getBytes();
    final static String EVIL_TEST_USER = "eviluser";
    final static byte[] EVIL_TEST_PWD = "unsecret".getBytes();

    final static List<InterceptHandler> EMPTY_OBSERVERS = Collections.emptyList();
    final static BrokerInterceptor NO_OBSERVERS_INTERCEPTOR = new BrokerInterceptor(EMPTY_OBSERVERS);
    
    DummyChannel m_session;
    ConnectMessage connMsg;
    ProtocolProcessor m_processor;
    
    IMessagesStore m_storageService;
    ISessionsStore m_sessionStore;
    SubscriptionsStore subscriptions;
    MockAuthenticator m_mockAuthenticator;
    
    @Before
    public void setUp() throws InterruptedException {
        connMsg = new ConnectMessage();
        connMsg.setProtocolVersion((byte) 0x03);

        m_session = new DummyChannel();

        //sleep to let the messaging batch processor to process the initEvent
        Thread.sleep(300);
        MemoryStorageService memStorage = new MemoryStorageService();
        memStorage.initStore();
        m_storageService = memStorage;
        m_sessionStore = memStorage;
        //m_storageService.initStore();
        
        Map<String, byte[]> users = new HashMap<>();
        users.put(TEST_USER, TEST_PWD);
        m_mockAuthenticator = new MockAuthenticator(users);

        subscriptions = new SubscriptionsStore();
        subscriptions.init(memStorage);
        m_processor = new ProtocolProcessor();
        m_processor.init(subscriptions, m_storageService, m_sessionStore, m_mockAuthenticator, true,
                new PermitAllAuthorizator(), NO_OBSERVERS_INTERCEPTOR);
    }


    @Test
    public void testPublish() throws InterruptedException {
        final Subscription subscription = new Subscription(FAKE_CLIENT_ID, 
                FAKE_TOPIC, AbstractMessage.QOSType.MOST_ONE, true);

        //subscriptions.matches(topic) redefine the method to return true
        SubscriptionsStore subs = new SubscriptionsStore() {
            @Override
            public List<Subscription> matches(String topic) {
                if (topic.equals(FAKE_TOPIC)) {
                    return Arrays.asList(subscription);
                } else {
                    throw new IllegalArgumentException("Expected " + FAKE_TOPIC + " buf found " + topic);
                }
            }
        };
        
        //simulate a connect that register a clientID to an IoSession
        MemoryStorageService storageService = new MemoryStorageService();
        storageService.initStore();
        subs.init(storageService);
        m_processor.init(subs, m_storageService, m_sessionStore, null, true, new PermitAllAuthorizator(), NO_OBSERVERS_INTERCEPTOR);
        ConnectMessage connectMessage = new ConnectMessage();
        connectMessage.setProtocolVersion((byte) 3);
        connectMessage.setClientID(FAKE_CLIENT_ID);
        m_sessionStore.createNewSession(FAKE_CLIENT_ID, false);
        connectMessage.setCleanSession(subscription.isCleanSession());
        m_processor.processConnect(m_session, connectMessage);
        
        
        //Exercise
        ByteBuffer buffer = ByteBuffer.allocate(5).put("Hello".getBytes());
        PublishMessage msg = new PublishMessage();
        msg.setTopicName(FAKE_TOPIC);
        msg.setQos(QOSType.MOST_ONE);
        msg.setPayload(buffer);
        msg.setRetainFlag(false);
        m_session.setAttribute(NettyChannel.ATTR_KEY_CLIENTID, "FakeCLI");
        m_processor.processPublish(m_session, msg);

        //Verify
        assertNotNull(m_session.getReceivedMessage());
        //TODO check received message attributes
    }
    
    @Test
    public void testPublishToMultipleSubscribers() throws InterruptedException {
        final Subscription subscription = new Subscription(FAKE_CLIENT_ID, 
                FAKE_TOPIC, AbstractMessage.QOSType.MOST_ONE, true);
        final Subscription subscriptionClient2 = new Subscription(FAKE_CLIENT_ID2, 
                FAKE_TOPIC, AbstractMessage.QOSType.MOST_ONE, true);

        //subscriptions.matches(topic) redefine the method to return true
        SubscriptionsStore subs = new SubscriptionsStore() {
            @Override
            public List<Subscription> matches(String topic) {
                if (topic.equals(FAKE_TOPIC)) {
                    return Arrays.asList(subscription, subscriptionClient2);
                } else {
                    throw new IllegalArgumentException("Expected " + FAKE_TOPIC + " buf found " + topic);
                }
            }
        };
        
        //simulate a connect that register a clientID to an IoSession
        MemoryStorageService storageService = new MemoryStorageService();
        storageService.initStore();
        subs.init(storageService);
        m_processor.init(subs, m_storageService, m_sessionStore, null, true, new PermitAllAuthorizator(), NO_OBSERVERS_INTERCEPTOR);
        
        MockReceiverChannel firstReceiverSession = new MockReceiverChannel();
        ConnectMessage connectMessage = new ConnectMessage();
        connectMessage.setProtocolVersion((byte) 3);
        connectMessage.setClientID(FAKE_CLIENT_ID);
        connectMessage.setCleanSession(subscription.isCleanSession());
        m_processor.processConnect(firstReceiverSession, connectMessage);
        
        //connect the second fake subscriber
        MockReceiverChannel secondReceiverSession = new MockReceiverChannel();
        ConnectMessage connectMessage2 = new ConnectMessage();
        connectMessage2.setProtocolVersion((byte) 3);
        connectMessage2.setClientID(FAKE_CLIENT_ID2);
        connectMessage2.setCleanSession(subscription.isCleanSession());
        m_processor.processConnect(secondReceiverSession, connectMessage2);
        
        //Exercise
        ByteBuffer buffer = ByteBuffer.allocate(5).put("Hello".getBytes());
        buffer.rewind();
        PublishMessage msg = new PublishMessage();
        msg.setTopicName(FAKE_TOPIC);
        msg.setQos(QOSType.MOST_ONE);
        msg.setPayload(buffer);
        msg.setRetainFlag(false);
        m_session.setAttribute(NettyChannel.ATTR_KEY_CLIENTID, "FakeCLI");
        m_processor.processPublish(m_session, msg);

        //Verify
        Thread.sleep(100); //ugly but we depend on the asynch that pull data from back disruptor
        PublishMessage pub2FirstSubscriber = (PublishMessage) firstReceiverSession.getMessage();
        assertNotNull(pub2FirstSubscriber);
        String firstMessageContent = DebugUtils.payload2Str(pub2FirstSubscriber.getPayload());
        assertEquals("Hello", firstMessageContent);
        
        PublishMessage pub2SecondSubscriber = (PublishMessage) secondReceiverSession.getMessage();
        assertNotNull(pub2SecondSubscriber);
        String secondMessageContent = DebugUtils.payload2Str(pub2SecondSubscriber.getPayload());
        assertEquals("Hello", secondMessageContent);
    }
    
    @Test
    public void testSubscribe() {
        //Exercise
        SubscribeMessage msg = new SubscribeMessage();
        msg.addSubscription(new SubscribeMessage.Couple(AbstractMessage.QOSType.MOST_ONE.byteValue(), FAKE_TOPIC));
        m_session.setAttribute(NettyChannel.ATTR_KEY_CLIENTID, FAKE_CLIENT_ID);
        m_session.setAttribute(NettyChannel.ATTR_KEY_CLEANSESSION, false);
        m_sessionStore.createNewSession(FAKE_CLIENT_ID, false);
        m_processor.processSubscribe(m_session, msg/*, FAKE_CLIENT_ID, false*/);

        //Verify
        assertTrue(m_session.getReceivedMessage() instanceof SubAckMessage);
        Subscription expectedSubscription = new Subscription(FAKE_CLIENT_ID, FAKE_TOPIC, AbstractMessage.QOSType.MOST_ONE, false);
        assertTrue(subscriptions.contains(expectedSubscription));
    }
    
    @Test
    public void testDoubleSubscribe() {
        SubscribeMessage msg = new SubscribeMessage();
        msg.addSubscription(new SubscribeMessage.Couple(AbstractMessage.QOSType.MOST_ONE.byteValue(), FAKE_TOPIC));
        m_session.setAttribute(NettyChannel.ATTR_KEY_CLIENTID, FAKE_CLIENT_ID);
        m_session.setAttribute(NettyChannel.ATTR_KEY_CLEANSESSION, false);
        m_sessionStore.createNewSession(FAKE_CLIENT_ID, false);
        assertEquals(0, subscriptions.size());
        
        m_processor.processSubscribe(m_session, msg);
                
        //Exercise
        m_processor.processSubscribe(m_session, msg);

        //Verify
        assertEquals(1, subscriptions.size());
        Subscription expectedSubscription = new Subscription(FAKE_CLIENT_ID, FAKE_TOPIC, AbstractMessage.QOSType.MOST_ONE, false);
        assertTrue(subscriptions.contains(expectedSubscription));
    }


    @Test
    public void testSubscribeWithBadFormattedTopic() {
        SubscribeMessage msg = new SubscribeMessage();
        msg.addSubscription(new SubscribeMessage.Couple(AbstractMessage.QOSType.MOST_ONE.byteValue(), BAD_FORMATTED_TOPIC));
        m_session.setAttribute(NettyChannel.ATTR_KEY_CLIENTID, FAKE_CLIENT_ID);
        m_session.setAttribute(NettyChannel.ATTR_KEY_CLEANSESSION, false);
        m_sessionStore.createNewSession(FAKE_CLIENT_ID, false);
        assertEquals(0, subscriptions.size());

        //Exercise
        m_processor.processSubscribe(m_session, msg);

        //Verify
        assertEquals(0, subscriptions.size());
        assertTrue(m_session.getReceivedMessage() instanceof SubAckMessage);
        List<QOSType> qosSubAcked = ((SubAckMessage) m_session.getReceivedMessage()).types();
        assertEquals(1, qosSubAcked.size());
        assertEquals(QOSType.FAILURE, qosSubAcked.get(0));
    }

    /*
     * Check topicFilter is a valid MQTT topic filter (issue 68)
     * */
    @Test
    public void testUnsubscribeWithBadFormattedTopic() {
        UnsubscribeMessage msg = new UnsubscribeMessage();
        msg.setMessageID(1);
        msg.addTopicFilter(BAD_FORMATTED_TOPIC);
        m_session.setAttribute(NettyChannel.ATTR_KEY_CLIENTID, FAKE_CLIENT_ID);
        m_session.setAttribute(NettyChannel.ATTR_KEY_CLEANSESSION, false);

        //Exercise
        m_processor.processUnsubscribe(m_session, msg);

        //Verify
        assertTrue(m_session.isClosed());
    }

    
    @Test
    public void testPublishOfRetainedMessage_afterNewSubscription() throws Exception {
        final CountDownLatch publishRecvSignal = new CountDownLatch(1);
        m_session = new DummyChannel() {
            @Override
            public void write(Object value) {
                try {
                    System.out.println("filterReceived class " + value.getClass().getName());
                    if (value instanceof PublishMessage) {
                        m_receivedMessage = (AbstractMessage) value;
                        publishRecvSignal.countDown();
                    }
                    
                    if (m_receivedMessage instanceof ConnAckMessage) {
                        ConnAckMessage buf = (ConnAckMessage) m_receivedMessage;
                        m_returnCode = buf.getReturnCode();
                    }
                } catch (Exception ex) {
                    throw new AssertionError("Wrong return code");
                }
            }   
        };
        
        //simulate a connect that register a clientID to an IoSession
        final Subscription subscription = new Subscription(FAKE_PUBLISHER_ID, 
                FAKE_TOPIC, AbstractMessage.QOSType.MOST_ONE, true);

        //subscriptions.matches(topic) redefine the method to return true
        SubscriptionsStore subs = new SubscriptionsStore() {
            @Override
            public List<Subscription> matches(String topic) {
                if (topic.equals(FAKE_TOPIC)) {
                    return Arrays.asList(subscription);
                } else {
                    throw new IllegalArgumentException("Expected " + FAKE_TOPIC + " buf found " + topic);
                }
            }
        };
        MemoryStorageService storageService = new MemoryStorageService();
        storageService.initStore();
        subs.init(storageService);

        //simulate a connect that register a clientID to an IoSession
        m_processor.init(subs, m_storageService, m_sessionStore, null, true, new PermitAllAuthorizator(), NO_OBSERVERS_INTERCEPTOR);
        ConnectMessage connectMessage = new ConnectMessage();
        connectMessage.setClientID(FAKE_PUBLISHER_ID);
        connectMessage.setProtocolVersion((byte) 3);
        connectMessage.setCleanSession(subscription.isCleanSession());
        m_processor.processConnect(m_session, connectMessage);
        ByteBuffer buffer = ByteBuffer.allocate(5).put("Hello".getBytes());
        PublishMessage pubmsg = new PublishMessage();
        pubmsg.setTopicName(FAKE_TOPIC);
        pubmsg.setQos(QOSType.MOST_ONE);
        pubmsg.setPayload(buffer);
        pubmsg.setRetainFlag(true);
        m_session.setAttribute(NettyChannel.ATTR_KEY_CLIENTID, FAKE_PUBLISHER_ID);
        m_processor.processPublish(m_session, pubmsg);
        m_session.setAttribute(NettyChannel.ATTR_KEY_CLEANSESSION, false);
        
        //Exercise
        SubscribeMessage msg = new SubscribeMessage();
        msg.addSubscription(new SubscribeMessage.Couple(QOSType.MOST_ONE.byteValue(), "#"));
        m_processor.processSubscribe(m_session, msg/*, FAKE_PUBLISHER_ID, false*/);
        
        //Verify
        //wait the latch
        assertTrue(publishRecvSignal.await(1, TimeUnit.SECONDS)); //no timeout
        assertNotNull(m_session.getReceivedMessage());
        assertTrue(m_session.getReceivedMessage() instanceof PublishMessage);
        PublishMessage pubMessage = (PublishMessage) m_session.getReceivedMessage();
        assertEquals(FAKE_TOPIC, pubMessage.getTopicName());
    }

    @Test
    public void testRepublishAndConsumePersistedMessages_onReconnect() {
        SubscriptionsStore subs = mock(SubscriptionsStore.class);
        List<Subscription> emptySubs = Collections.emptyList();
        when(subs.matches(anyString())).thenReturn(emptySubs);
        PublishEvent retainedMessage = new PublishEvent("/topic", QOSType.EXACTLY_ONCE,
                ByteBuffer.wrap("Hello".getBytes()), true, FAKE_PUBLISHER_ID, 120);
        m_storageService.storePublishForFuture(retainedMessage);

        m_processor.init(subs, m_storageService, m_sessionStore, null, true, new PermitAllAuthorizator(), NO_OBSERVERS_INTERCEPTOR);
        ConnectMessage connectMessage = new ConnectMessage();
        connectMessage.setClientID(FAKE_PUBLISHER_ID);
        connectMessage.setProtocolVersion((byte) 3);
        connectMessage.setCleanSession(false);
        m_processor.processConnect(m_session, connectMessage);

        //Verify no messages are still stored
        assertTrue(m_storageService.listMessagesInSession(FAKE_PUBLISHER_ID).isEmpty());
    }
    
    @Test
    public void publishNoPublishToInactiveSession() {
        //create an inactive session for Subscriber
        m_sessionStore.createNewSession("Subscriber", false).deactivate();

        SubscriptionsStore mockedSubscriptions = mock(SubscriptionsStore.class);
        Subscription inactiveSub = new Subscription("Subscriber", "/topic", QOSType.LEAST_ONE, false); 
        List<Subscription> inactiveSubscriptions = Arrays.asList(inactiveSub);
        when(mockedSubscriptions.matches(eq("/topic"))).thenReturn(inactiveSubscriptions);
        m_processor = new ProtocolProcessor();
        m_processor.init(mockedSubscriptions, m_storageService, m_sessionStore, null, true, new PermitAllAuthorizator(), NO_OBSERVERS_INTERCEPTOR);
        
        //Exercise
        ByteBuffer buffer = ByteBuffer.allocate(5).put("Hello".getBytes());
        PublishMessage msg = new PublishMessage();
        msg.setTopicName("/topic");
        msg.setQos(QOSType.MOST_ONE);
        msg.setPayload(buffer);
        msg.setRetainFlag(true);
        m_session.setAttribute(NettyChannel.ATTR_KEY_CLIENTID, "Publisher");
        m_processor.processPublish(m_session, msg);

        //Verify no message is received
        assertNull(m_session.getReceivedMessage());
    }
    
    
    @Test
    public void publishToAnInactiveSubscriptionsCleanSession() {
        //create an inactive session for Subscriber
        m_sessionStore.createNewSession("Subscriber", false).deactivate();
        SubscriptionsStore mockedSubscriptions = mock(SubscriptionsStore.class);
        Subscription inactiveSub = new Subscription("Subscriber", "/topic", QOSType.LEAST_ONE, true); 
        List<Subscription> inactiveSubscriptions = Arrays.asList(inactiveSub);
        when(mockedSubscriptions.matches(eq("/topic"))).thenReturn(inactiveSubscriptions);
        m_processor = new ProtocolProcessor();
        m_processor.init(mockedSubscriptions, m_storageService, m_sessionStore, null, true, new PermitAllAuthorizator(),
                NO_OBSERVERS_INTERCEPTOR);

        //Exercise
        ByteBuffer buffer = ByteBuffer.allocate(5).put("Hello".getBytes());
        PublishMessage msg = new PublishMessage();
        msg.setTopicName("/topic");
        msg.setQos(QOSType.MOST_ONE);
        msg.setPayload(buffer);
        msg.setRetainFlag(true);
        m_session.setAttribute(NettyChannel.ATTR_KEY_CLIENTID, "Publisher");
        m_processor.processPublish(m_session, msg);

        //Verify no message is received
        assertNull(m_session.getReceivedMessage());
    }
    
    
    /**
     * Verify that receiving a publish with retained message and with Q0S = 0 
     * clean the existing retained messages for that topic.
     */
    @Test
    public void testCleanRetainedStoreAfterAQoS0AndRetainedTrue() {
        //force a connect
        connMsg.setClientID("Publisher");
        m_processor.processConnect(m_session, connMsg);
        //prepare and existing retained store
        m_session.setAttribute(NettyChannel.ATTR_KEY_CLIENTID, "Publisher");
        ByteBuffer payload = ByteBuffer.allocate(5).put("Hello".getBytes());
        PublishMessage msg = new PublishMessage();
        msg.setTopicName(FAKE_TOPIC);
        msg.setQos(QOSType.LEAST_ONE);
        msg.setPayload(payload);
        msg.setRetainFlag(true);
        msg.setMessageID(100);
        m_processor.processPublish(m_session, msg);
        
        //Exercise
        PublishMessage cleanPubMsg = new PublishMessage();
        cleanPubMsg.setTopicName(FAKE_TOPIC);
        cleanPubMsg.setPayload(payload);
        cleanPubMsg.setQos(QOSType.MOST_ONE);
        cleanPubMsg.setRetainFlag(true);
        m_processor.processPublish(m_session, cleanPubMsg);
        
        //Verify
        Collection<IMessagesStore.StoredMessage> messages = m_storageService.searchMatching(new IMatchingCondition() {
            public boolean match(String key) {
                return  SubscriptionsStore.matchTopics(key, FAKE_TOPIC);
            }
        });
        assertTrue(messages.isEmpty());
    }

    List<PublishEvent> publishedForwarded = new ArrayList<>();

    @Test
    public void testForwardPublishWithCorrectQos() {
        ByteBuffer payload = ByteBuffer.wrap("Hello world MQTT!!".getBytes());
        PublishEvent forwardPublish = new PublishEvent("a/b", QOSType.EXACTLY_ONCE, payload, true, "Publisher", 1);
        IMessagesStore memoryMessageStore = new MemoryStorageService();
        ISessionsStore sessionsStore = new MemorySessionStore(memoryMessageStore);
        sessionsStore.createNewSession("Sub A", false).activate();
        sessionsStore.createNewSession("Sub B", false).activate();

        Subscription subQos1 = new Subscription("Sub A", "a/b", QOSType.LEAST_ONE, false);
        Subscription subQos2 = new Subscription("Sub B", "a/+", QOSType.EXACTLY_ONCE, false);
        SubscriptionsStore subscriptions = new SubscriptionsStore();
        subscriptions.init(sessionsStore);
        subscriptions.add(subQos1);
        subscriptions.add(subQos2);


        ProtocolProcessor processor = new ProtocolProcessor() {
            @Override
            protected void sendPublish(String clientId, String topic, AbstractMessage.QOSType qos, ByteBuffer message,
                                       boolean retained, Integer messageID) {
                publishedForwarded.add(new PublishEvent(topic, qos, message, retained, clientId, messageID));
            }
        };
        processor.init(subscriptions, memoryMessageStore, sessionsStore, null, true, null, NO_OBSERVERS_INTERCEPTOR);

        //Exercise
        processor.forward2Subscribers(forwardPublish);

        //Verify
        assertEquals(2, publishedForwarded.size());
        assertEquals(subQos1.getClientId(), publishedForwarded.get(0).getClientID());
        assertEquals(subQos1.getRequestedQos(), publishedForwarded.get(0).getQos());
        assertEquals(subQos2.getClientId(), publishedForwarded.get(1).getClientID());
        assertEquals(subQos2.getRequestedQos(), publishedForwarded.get(1).getQos());
    }
}
