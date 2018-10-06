package io.moquette.broker;

import io.moquette.persistence.MemoryStorageService;
import io.moquette.server.netty.NettyUtils;
import io.moquette.spi.ISessionsStore;
import io.moquette.spi.impl.MockAuthenticator;
import io.moquette.spi.impl.SessionsRepository;
import io.moquette.spi.impl.security.PermitAllAuthorizatorPolicy;
import io.moquette.spi.impl.subscriptions.CTrieSubscriptionDirectory;
import io.moquette.spi.impl.subscriptions.ISubscriptionsDirectory;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import io.moquette.spi.security.IAuthenticator;
import io.moquette.spi.security.IAuthorizatorPolicy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.*;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;

import static io.moquette.broker.PostOfficePublishTest.ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID;
import static io.moquette.broker.PostOfficePublishTest.SUBSCRIBER_ID;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import static io.netty.handler.codec.mqtt.MqttQoS.EXACTLY_ONCE;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PostOfficeSubscribeTest {

    private static final String FAKE_CLIENT_ID = "FAKE_123";
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
    private IAuthenticator mockAuthenticator;
    private SessionRegistry sessionRegistry;

    @Before
    public void setUp() {
        connectMessage = MqttMessageBuilders.connect()
            .clientId(FAKE_CLIENT_ID)
            .build();

        BrokerConfiguration config = new BrokerConfiguration(true, true, false);

        prepareSUT();
        createMQTTConnection(config);
    }

    private void createMQTTConnection(BrokerConfiguration config) {
        channel = new EmbeddedChannel();
        connection = createMQTTConnection(config, channel);
    }

    private void prepareSUT() {
        MemoryStorageService memStorage = new MemoryStorageService(null, null);
        ISessionsStore sessionStore = memStorage.sessionsStore();
        mockAuthenticator = new MockAuthenticator(singleton(FAKE_CLIENT_ID), singletonMap(TEST_USER, TEST_PWD));

        subscriptions = new CTrieSubscriptionDirectory();
        SessionsRepository sessionsRepository = new SessionsRepository(sessionStore, null);
        subscriptions.init(sessionsRepository);

        sut = new PostOffice(subscriptions, new PermitAllAuthorizatorPolicy(), new MemoryRetainedRepository());
        sessionRegistry = new SessionRegistry(subscriptions, sut);
        sut.init(sessionRegistry);
    }

    private MQTTConnection createMQTTConnection(BrokerConfiguration config, Channel channel) {
        return new MQTTConnection(channel, config, mockAuthenticator, sessionRegistry, sut);
    }

    protected void connect() {
        MqttConnectMessage connectMessage = MqttMessageBuilders.connect()
            .clientId(FAKE_CLIENT_ID)
            .build();
        connection.processConnect(connectMessage);
        MqttConnAckMessage connAck = channel.readOutbound();
        assertEquals("Connect must be accepted", CONNECTION_ACCEPTED, connAck.variableHeader().connectReturnCode());
    }

    @Test
    public void testSubscribe() {
        connection.processConnect(connectMessage);
        assertConnectAccepted(channel);

        // Exercise & verify
        subscribe(channel, NEWS_TOPIC, AT_MOST_ONCE);
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

    protected void subscribe(MQTTConnection connection, String topic, MqttQoS desiredQos) {
        EmbeddedChannel channel = (EmbeddedChannel) connection.channel;
        MqttSubscribeMessage subscribe = MqttMessageBuilders.subscribe()
            .addSubscription(desiredQos, topic)
            .messageId(1)
            .build();
        sut.subscribeClientToTopics(subscribe, connection.getClientId(), null, connection);

        MqttSubAckMessage subAck = channel.readOutbound();
        assertEquals(desiredQos.value(), (int) subAck.payload().grantedQoSLevels().get(0));

        final String clientId = connection.getClientId();
        Subscription expectedSubscription = new Subscription(clientId, new Topic(topic), desiredQos);

        final Set<Subscription> matchedSubscriptions = subscriptions.matchWithoutQosSharpening(new Topic(topic));
        assertEquals(1, matchedSubscriptions.size());
        final Subscription onlyMatchedSubscription = matchedSubscriptions.iterator().next();
        assertEquals(expectedSubscription, onlyMatchedSubscription);
    }

    @Test
    public void testSubscribedToNotAuthorizedTopic() {
        NettyUtils.userName(channel, FAKE_USER_NAME);

        IAuthorizatorPolicy prohibitReadOnNewsTopic = mock(IAuthorizatorPolicy.class);
        when(prohibitReadOnNewsTopic.canRead(eq(new Topic(NEWS_TOPIC)), eq(FAKE_USER_NAME), eq(FAKE_CLIENT_ID)))
            .thenReturn(false);

        sut = new PostOffice(subscriptions, prohibitReadOnNewsTopic, new MemoryRetainedRepository());
        sut.init(sessionRegistry);

        connection.processConnect(connectMessage);
        assertConnectAccepted(channel);

        //Exercise
        MqttSubscribeMessage subscribe = MqttMessageBuilders.subscribe()
            .addSubscription(AT_MOST_ONCE, NEWS_TOPIC)
            .messageId(1)
            .build();
        sut.subscribeClientToTopics(subscribe, FAKE_CLIENT_ID, FAKE_USER_NAME, connection);

        // Verify
        MqttSubAckMessage subAckMsg = channel.readOutbound();
        verifyFailureQos(subAckMsg);
    }


    private void verifyFailureQos(MqttSubAckMessage subAckMsg) {
        List<Integer> grantedQoSes = subAckMsg.payload().grantedQoSLevels();
        assertEquals(1, grantedQoSes.size());
        assertTrue(grantedQoSes.contains(MqttQoS.FAILURE.value()));
    }

    @Test
    public void testDoubleSubscribe() {
        connection.processConnect(connectMessage);
        assertConnectAccepted(channel);
        assertEquals("After CONNECT subscription MUST be empty", 0, subscriptions.size());
        subscribe(channel, NEWS_TOPIC, AT_MOST_ONCE);
        assertEquals("After /news subscribe, subscription MUST contain it",1, subscriptions.size());

        //Exercise & verify
        subscribe(channel, NEWS_TOPIC, AT_MOST_ONCE);
    }

    @Test
    public void testSubscribeWithBadFormattedTopic() {
        connection.processConnect(connectMessage);
        assertConnectAccepted(channel);
        assertEquals("After CONNECT subscription MUST be empty", 0, subscriptions.size());

        //Exercise
        MqttSubscribeMessage subscribe = MqttMessageBuilders.subscribe()
            .addSubscription(AT_MOST_ONCE, BAD_FORMATTED_TOPIC)
            .messageId(1)
            .build();
        this.sut.subscribeClientToTopics(subscribe, FAKE_CLIENT_ID, FAKE_USER_NAME, connection);
        MqttSubAckMessage subAckMsg = channel.readOutbound();

        assertEquals("Bad topic CAN'T add any subscription",0, subscriptions.size());
        verifyFailureQos(subAckMsg);
    }

    @Test
    public void testReceiveRetainedPublishRespectingSubscriptionQoSAndNotPublisher() {
        // publisher publish a retained message on topic /news
        connection.processConnect(connectMessage);
        ConnectionTestUtils.assertConnectAccepted(channel);
        final ByteBuf payload = Unpooled.copiedBuffer("Hello world!", Charset.defaultCharset());
        final MqttPublishMessage retainedPubQoS1Msg = MqttMessageBuilders.publish()
            .payload(payload.retainedDuplicate())
            .qos(MqttQoS.AT_LEAST_ONCE)
            .topicName(NEWS_TOPIC).build();
        sut.receivedPublishQos1(connection, new Topic(NEWS_TOPIC), TEST_USER, payload, 1, true,
            retainedPubQoS1Msg);

        // subscriber connects subscribe to topic /news and receive the last retained message
        EmbeddedChannel subChannel = new EmbeddedChannel();
        MQTTConnection subConn = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID, subChannel);
        subConn.processConnect(ConnectionTestUtils.buildConnect(SUBSCRIBER_ID));
        ConnectionTestUtils.assertConnectAccepted(subChannel);
        subscribe(subConn, NEWS_TOPIC, MqttQoS.AT_MOST_ONCE);

        // Verify publish is received
        ConnectionTestUtils.verifyReceiveRetainedPublish(subChannel, NEWS_TOPIC, "Hello world!", MqttQoS.AT_MOST_ONCE);
    }

    @Test
    public void testLowerTheQosToTheRequestedBySubscription() {
        Subscription subQos1 = new Subscription("Sub A", new Topic("a/b"), MqttQoS.AT_LEAST_ONCE);
        assertEquals(MqttQoS.AT_LEAST_ONCE, PostOffice.lowerQosToTheSubscriptionDesired(subQos1, EXACTLY_ONCE));

        Subscription subQos2 = new Subscription("Sub B", new Topic("a/+"), EXACTLY_ONCE);
        assertEquals(EXACTLY_ONCE, PostOffice.lowerQosToTheSubscriptionDesired(subQos2, EXACTLY_ONCE));
    }
}
