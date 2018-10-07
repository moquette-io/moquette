package io.moquette.broker;

import io.moquette.persistence.MemoryStorageService;
import io.moquette.spi.ISessionsStore;
import io.moquette.spi.impl.MockAuthenticator;
import io.moquette.spi.impl.SessionsRepository;
import io.moquette.spi.impl.security.PermitAllAuthorizatorPolicy;
import io.moquette.spi.impl.subscriptions.CTrieSubscriptionDirectory;
import io.moquette.spi.impl.subscriptions.ISubscriptionsDirectory;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import io.moquette.spi.security.IAuthenticator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.*;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Set;

import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import static java.util.Collections.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PostOfficeUnsubscribeTest {

    private static final String FAKE_CLIENT_ID = "FAKE_123";
    private static final String TEST_USER = "fakeuser";
    private static final String TEST_PWD = "fakepwd";
    static final String NEWS_TOPIC = "/news";
    private static final String BAD_FORMATTED_TOPIC = "#MQTTClient";

    private MQTTConnection connection;
    private EmbeddedChannel channel;
    private PostOffice sut;
    private ISubscriptionsDirectory subscriptions;
    private MqttConnectMessage connectMessage;
    private IAuthenticator mockAuthenticator;
    private SessionRegistry sessionRegistry;
    public static final BrokerConfiguration CONFIG = new BrokerConfiguration(true, true, false);

    @Before
    public void setUp() {
        connectMessage = MqttMessageBuilders.connect()
            .clientId(FAKE_CLIENT_ID)
            .build();

        prepareSUT();
        createMQTTConnection(CONFIG);
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
        ConnectionTestUtils.assertConnectAccepted(channel);
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
    public void testUnsubscribeWithBadFormattedTopic() {
        connect();

        // Exercise
        sut.unsubscribe(singletonList(BAD_FORMATTED_TOPIC), connection, 1);

        // Verify
        assertFalse("Unsubscribe with bad topic MUST close drop the connection, (issue 68)", channel.isOpen());
    }

    @Test
    public void testDontNotifyClientSubscribedToTopicAfterDisconnectedAndReconnectOnSameChannel() {
        connect();
        subscribe(connection, NEWS_TOPIC, AT_MOST_ONCE);

        // publish on /news
        final ByteBuf payload = Unpooled.copiedBuffer("Hello world!", Charset.defaultCharset());
        sut.receivedPublishQos0(new Topic(NEWS_TOPIC), TEST_USER, TEST_PWD, payload, false);

        ConnectionTestUtils.verifyPublishIsReceived(channel, AT_MOST_ONCE, "Hello world!");

        unsubscribeAndVerifyAck(NEWS_TOPIC);

        // publish on /news
        final ByteBuf payload2 = Unpooled.copiedBuffer("Hello world!", Charset.defaultCharset());
        sut.receivedPublishQos0(new Topic(NEWS_TOPIC), TEST_USER, TEST_PWD, payload2, false);

        ConnectionTestUtils.verifyNoPublishIsReceived(channel);
    }

    protected void unsubscribeAndVerifyAck(String topic) {
        final int messageId = 1;

        sut.unsubscribe(Collections.singletonList(topic), connection, messageId);

        MqttUnsubAckMessage unsubAckMessageAck = channel.readOutbound();
        assertEquals("Unsubscribe must be accepted", messageId, unsubAckMessageAck.variableHeader().messageId());
    }

    @Test
    public void testDontNotifyClientSubscribedToTopicAfterDisconnectedAndReconnectOnNewChannel() {
        connect();
        subscribe(connection, NEWS_TOPIC, AT_MOST_ONCE);
        // publish on /news
        final ByteBuf payload = Unpooled.copiedBuffer("Hello world!", Charset.defaultCharset());
        sut.receivedPublishQos0(new Topic(NEWS_TOPIC), TEST_USER, TEST_PWD, payload, false);

        ConnectionTestUtils.verifyPublishIsReceived(channel, AT_MOST_ONCE, "Hello world!");

        unsubscribeAndVerifyAck(NEWS_TOPIC);
        connection.processDisconnect(null);

        // connect on another channel
        EmbeddedChannel anotherChannel = new EmbeddedChannel();
        MQTTConnection anotherConnection = createMQTTConnection(CONFIG, anotherChannel);
        anotherConnection.processConnect(connectMessage);
        ConnectionTestUtils.assertConnectAccepted(anotherChannel);

        // publish on /news
        final ByteBuf payload2 = Unpooled.copiedBuffer("Hello world!", Charset.defaultCharset());
        sut.receivedPublishQos0(new Topic(NEWS_TOPIC), TEST_USER, TEST_PWD, payload2, false);

        ConnectionTestUtils.verifyNoPublishIsReceived(anotherChannel);
    }
}
