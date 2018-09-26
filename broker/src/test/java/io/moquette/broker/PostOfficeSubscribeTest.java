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
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.*;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
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

    @Before
    public void setUp() {
        BrokerConfiguration config = new BrokerConfiguration(true, true, false);

        createMQTTConnection(config);

        connectMessage = MqttMessageBuilders.connect()
            .clientId(FAKE_CLIENT_ID)
            .build();
    }

    private void createMQTTConnection(BrokerConfiguration config) {
        channel = new EmbeddedChannel();
        connection = createMQTTConnection(config, channel);
    }

    private MQTTConnection createMQTTConnection(BrokerConfiguration config, Channel channel) {
        MemoryStorageService memStorage = new MemoryStorageService(null, null);
        ISessionsStore sessionStore = memStorage.sessionsStore();
        IAuthenticator mockAuthenticator = new MockAuthenticator(singleton(FAKE_CLIENT_ID), singletonMap(TEST_USER, TEST_PWD));

        subscriptions = new CTrieSubscriptionDirectory();
        SessionsRepository sessionsRepository = new SessionsRepository(sessionStore, null);
        subscriptions.init(sessionsRepository);

        sut = new PostOffice(subscriptions, new PermitAllAuthorizatorPolicy(), new MemoryRetainedRepository());
        SessionRegistry sessionRegistry = new SessionRegistry(subscriptions, sut);
        return new MQTTConnection(channel, config, mockAuthenticator, sessionRegistry, sut);
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

    @Test
    public void testSubscribedToNotAuthorizedTopic() {
        NettyUtils.userName(channel, FAKE_USER_NAME);

        IAuthorizatorPolicy prohibitReadOnNewsTopic = mock(IAuthorizatorPolicy.class);
        when(prohibitReadOnNewsTopic.canRead(eq(new Topic(NEWS_TOPIC)), eq(FAKE_USER_NAME), eq(FAKE_CLIENT_ID)))
            .thenReturn(false);

        sut = new PostOffice(subscriptions, prohibitReadOnNewsTopic, new MemoryRetainedRepository());

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
}
