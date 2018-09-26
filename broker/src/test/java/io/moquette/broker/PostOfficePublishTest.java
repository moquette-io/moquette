package io.moquette.broker;

import io.moquette.persistence.MemoryStorageService;
import io.moquette.server.netty.NettyUtils;
import io.moquette.spi.ISessionsStore;
import io.moquette.spi.impl.DebugUtils;
import io.moquette.spi.impl.MockAuthenticator;
import io.moquette.spi.impl.SessionsRepository;
import io.moquette.spi.impl.security.PermitAllAuthorizatorPolicy;
import io.moquette.spi.impl.subscriptions.CTrieSubscriptionDirectory;
import io.moquette.spi.impl.subscriptions.ISubscriptionsDirectory;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.*;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_LEAST_ONCE;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;

public class PostOfficePublishTest {

    private static final String FAKE_CLIENT_ID = "FAKE_123";
    private static final String FAKE_CLIENT_ID2 = "FAKE_456";
    private static final String TEST_USER = "fakeuser";
    private static final String TEST_PWD = "fakepwd";
    private static final String NEWS_TOPIC = "/news";
    private static final String BAD_FORMATTED_TOPIC = "#MQTTClient";

    private MQTTConnection connection;
    private EmbeddedChannel channel;
    private PostOffice sut;
    private ISubscriptionsDirectory subscriptions;
    public static final String FAKE_USER_NAME = "UnAuthUser";
    private MqttConnectMessage connectMessage;
    private SessionRegistry sessionRegistry;
    private MockAuthenticator mockAuthenticator;
    private static final BrokerConfiguration ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID =
        new BrokerConfiguration(true, true, false);
    private MemoryRetainedRepository retainedRepository;

    @Before
    public void setUp() {
        sessionRegistry = initPostOfficeAndSubsystems();

        mockAuthenticator = new MockAuthenticator(singleton(FAKE_CLIENT_ID), singletonMap(TEST_USER, TEST_PWD));
        connection = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID);

        connectMessage = buildConnect(FAKE_CLIENT_ID);
    }

    private MqttConnectMessage buildConnect(String clientId) {
        return MqttMessageBuilders.connect()
            .clientId(clientId)
            .build();
    }

    private MQTTConnection createMQTTConnection(BrokerConfiguration config) {
        channel = new EmbeddedChannel();
        return createMQTTConnection(config, channel);
    }

    private MQTTConnection createMQTTConnection(BrokerConfiguration config, Channel channel) {
        return new MQTTConnection(channel, config, mockAuthenticator, sessionRegistry, sut);
    }

    private SessionRegistry initPostOfficeAndSubsystems() {
        MemoryStorageService memStorage = new MemoryStorageService(null, null);
        ISessionsStore sessionStore = memStorage.sessionsStore();

        subscriptions = new CTrieSubscriptionDirectory();
        SessionsRepository sessionsRepository = new SessionsRepository(sessionStore, null);
        subscriptions.init(sessionsRepository);
        retainedRepository = new MemoryRetainedRepository();

        sut = new PostOffice(subscriptions, new PermitAllAuthorizatorPolicy(), retainedRepository);
        SessionRegistry sessionRegistry = new SessionRegistry(subscriptions, sut);
        sut.init(sessionRegistry);
        return sessionRegistry;
    }

    @Test
    public void testPublishQoS0ToItself() {
        connection.processConnect(connectMessage);
        assertConnectAccepted(channel);

        // subscribe
        final MqttQoS qos = AT_MOST_ONCE;
        final String newsTopic = NEWS_TOPIC;
        subscribe(qos, newsTopic, connection);

        // Exercise
        final ByteBuf payload = Unpooled.copiedBuffer("Hello world!", Charset.defaultCharset());
        sut.receivedPublishQos0(new Topic(NEWS_TOPIC), TEST_USER, FAKE_CLIENT_ID, payload, false);

        // Verify
        verifyReceivePublish(channel, NEWS_TOPIC, "Hello world!");
    }

    private void subscribe(MqttQoS topic, String newsTopic, MQTTConnection connection) {
        MqttSubscribeMessage subscribe = MqttMessageBuilders.subscribe()
            .addSubscription(topic, newsTopic)
            .messageId(1)
            .build();
        sut.subscribeClientToTopics(subscribe, connection.getClientId(), null, this.connection);

        MqttSubAckMessage subAck = ((EmbeddedChannel) this.connection.channel).readOutbound();
        assertEquals(topic.value(), (int) subAck.payload().grantedQoSLevels().get(0));
    }

    private void assertConnectAccepted(EmbeddedChannel channel) {
        MqttConnAckMessage connAck = channel.readOutbound();
        final MqttConnectReturnCode connAckReturnCode = connAck.variableHeader().connectReturnCode();
        assertEquals("Connect must be accepted", CONNECTION_ACCEPTED, connAckReturnCode);
    }

    protected void subscribe(EmbeddedChannel channel, String topic, MqttQoS desiredQos) {
        MqttSubscribeMessage subscribe = MqttMessageBuilders.subscribe()
            .addSubscription(desiredQos, topic)
            .messageId(1)
            .build();
        sut.subscribeClientToTopics(subscribe, FAKE_CLIENT_ID, null, connection);

        MqttSubAckMessage subAck = channel.readOutbound();
        assertEquals(desiredQos.value(), (int) subAck.payload().grantedQoSLevels().get(0));

        final String clientId = NettyUtils.clientID(channel);
        Subscription expectedSubscription = new Subscription(clientId, new Topic(topic), desiredQos);

        final Set<Subscription> matchedSubscriptions = subscriptions.matchWithoutQosSharpening(new Topic(topic));
        assertEquals(1, matchedSubscriptions.size());
        final Subscription onlyMatchedSubscription = matchedSubscriptions.iterator().next();
        assertEquals(expectedSubscription, onlyMatchedSubscription);
    }

    @Test
    public void testPublishToMultipleSubscribers() {
        final Set<String> clientIds = new HashSet<>(Arrays.asList(FAKE_CLIENT_ID, FAKE_CLIENT_ID2));
        mockAuthenticator = new MockAuthenticator(clientIds, singletonMap(TEST_USER, TEST_PWD));
        EmbeddedChannel channel1 = new EmbeddedChannel();
        MQTTConnection connection1 = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID, channel1);
        connection1.processConnect(buildConnect(FAKE_CLIENT_ID));
        assertConnectAccepted(channel1);

        EmbeddedChannel channel2 = new EmbeddedChannel();
        MQTTConnection connection2 = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID, channel2);
        connection2.processConnect(buildConnect(FAKE_CLIENT_ID2));
        assertConnectAccepted(channel2);

        // subscribe
        final MqttQoS qos = AT_MOST_ONCE;
        final String newsTopic = NEWS_TOPIC;
        subscribe(qos, newsTopic, connection1);
        subscribe(qos, newsTopic, connection2);

        // Exercise
        final ByteBuf payload = Unpooled.copiedBuffer("Hello world!", Charset.defaultCharset());
        sut.receivedPublishQos0(new Topic(NEWS_TOPIC), TEST_USER, FAKE_CLIENT_ID, payload, false);

        // Verify
        verifyReceivePublish(channel1, NEWS_TOPIC, "Hello world!");
        verifyReceivePublish(channel2, NEWS_TOPIC, "Hello world!");
    }

    private void verifyReceivePublish(EmbeddedChannel channel1, String expectedTopic, String expectedContent) {
        MqttPublishMessage receivedPublish = channel1.readOutbound();
        final String decodedPayload = DebugUtils.payload2Str(receivedPublish.payload());
        assertEquals(expectedContent, decodedPayload);
        assertEquals(expectedTopic, receivedPublish.variableHeader().topicName());
    }


    @Test
    public void testPublishWithEmptyPayloadClearRetainedStore() {
        connection.processConnect(connectMessage);
        assertConnectAccepted(channel);

        this.retainedRepository.retain(new Topic(NEWS_TOPIC), MqttMessageBuilders.publish()
            .payload(ByteBufUtil.writeAscii(UnpooledByteBufAllocator.DEFAULT, "Hello world!"))
            .qos(AT_LEAST_ONCE)
            .build());

        // Exercise
        final ByteBuf anyPayload = Unpooled.copiedBuffer("Any payload", Charset.defaultCharset());
        sut.receivedPublishQos0(new Topic(NEWS_TOPIC), TEST_USER, FAKE_CLIENT_ID, anyPayload, true);

        // Verify
        assertTrue("QoS0 MUST clean retained message for topic", retainedRepository.isEmtpy());
    }


}
