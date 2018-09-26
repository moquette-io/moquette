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
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.*;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
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

    @Before
    public void setUp() {
        connectMessage = MqttMessageBuilders.connect()
            .clientId(FAKE_CLIENT_ID)
            .build();

        BrokerConfiguration config = new BrokerConfiguration(true, true, false);

        createMQTTConnection(config);
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

    protected void connect() {
        MqttConnectMessage connectMessage = MqttMessageBuilders.connect()
            .clientId(FAKE_CLIENT_ID)
            .build();
        connection.processConnect(connectMessage);
        MqttConnAckMessage connAck = channel.readOutbound();
        assertEquals("Connect must be accepted", CONNECTION_ACCEPTED, connAck.variableHeader().connectReturnCode());
    }

    protected void subscribe(String topic, MqttQoS desiredQos) {
        subscribe(channel, topic, desiredQos);
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
        final Subscription onlyMatcheSubscription = matchedSubscriptions.iterator().next();
        assertEquals(expectedSubscription, onlyMatcheSubscription);
    }

    @Test
    public void testUnsubscribeWithBadFormattedTopic() {
        connection.processConnect(connectMessage);
        MqttConnAckMessage connAck = channel.readOutbound();
        final MqttConnectReturnCode connAckReturnCode = connAck.variableHeader().connectReturnCode();
        assertEquals("Connect must be accepted", CONNECTION_ACCEPTED, connAckReturnCode);

        // Exercise
        sut.unsubscribe(singletonList(BAD_FORMATTED_TOPIC), connection);

        // Verify
        assertFalse("Unsubscribe with bad topic MUST close drop the connection, (issue 68)", channel.isOpen());
    }
}
