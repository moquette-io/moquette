package io.moquette.broker;

import io.moquette.persistence.MemoryStorageService;
import io.moquette.spi.ISessionsStore;
import io.moquette.spi.impl.MockAuthenticator;
import io.moquette.spi.impl.SessionsRepository;
import io.moquette.spi.impl.security.PermitAllAuthorizatorPolicy;
import io.moquette.spi.impl.subscriptions.CTrieSubscriptionDirectory;
import io.moquette.spi.impl.subscriptions.ISubscriptionsDirectory;
import io.moquette.spi.security.IAuthenticator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.junit.Before;
import org.junit.Test;

import static io.moquette.spi.impl.NettyChannelAssertions.assertEqualsConnAck;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.*;

public class MQTTConnectionConnectTest {

    static final String FAKE_CLIENT_ID = "FAKE_123";
    static final String TEST_USER = "fakeuser";
    static final String TEST_PWD = "fakepwd";

    private MQTTConnection sut;
    private EmbeddedChannel channel;
    private SessionRegistry sessionRegistry;

    @Before
    public void setUp() {
        BrokerConfiguration config = new BrokerConfiguration(true, true, false);

        MemoryStorageService memStorage = new MemoryStorageService(null, null);
//        messagesStore = memStorage.messagesStore();
        ISessionsStore sessionStore = memStorage.sessionsStore();
        IAuthenticator mockAuthenticator = new MockAuthenticator(singleton(FAKE_CLIENT_ID), singletonMap(TEST_USER, TEST_PWD));

        ISubscriptionsDirectory subscriptions = new CTrieSubscriptionDirectory();
        SessionsRepository sessionsRepository = new SessionsRepository(sessionStore, null);
        subscriptions.init(sessionsRepository);

        channel = new EmbeddedChannel();
        final PostOffice postOffice = new PostOffice(subscriptions, new PermitAllAuthorizatorPolicy());
        sessionRegistry = new SessionRegistry(subscriptions, postOffice);
        sut = new MQTTConnection(channel, config, mockAuthenticator, sessionRegistry, postOffice);
    }

    @Test
    public void testZeroByteClientIdWithCleanSession() {
        // Connect message with clean session set to true and client id is null.
        MqttConnectMessage msg = MqttMessageBuilders.connect()
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .clientId(null)
            .cleanSession(true)
            .build();

        sut.processConnect(msg);
        assertEqualsConnAck("Connection must be accepted", CONNECTION_ACCEPTED, channel.readOutbound());
        assertNotNull("unique clientid must be generated", sut.getClientId());
        assertTrue("clean session flag must be true", sessionRegistry.retrieve(sut.getClientId()).isClean());
        assertTrue("Connection must be open", channel.isOpen());
    }

    @Test
    public void invalidAuthentication() {
        MqttConnectMessage msg = MqttMessageBuilders.connect()
            .protocolVersion(MqttVersion.MQTT_3_1)
            .cleanSession(true)
            .clientId(FAKE_CLIENT_ID)
            .username(TEST_USER + "_fake")
            .password(TEST_PWD)
            .build();

        // Exercise
        sut.processConnect(msg);

        // Verify
        assertEqualsConnAck(CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, channel.readOutbound());
        assertFalse("Connection must be closed by the broker", channel.isOpen());
    }
}
