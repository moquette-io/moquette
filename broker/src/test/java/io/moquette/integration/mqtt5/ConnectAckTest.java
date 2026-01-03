package io.moquette.integration.mqtt5;

import io.moquette.BrokerConstants;
import io.moquette.broker.config.FluentConfig;
import io.moquette.broker.config.IConfig;
import io.moquette.testclient.Client;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static io.moquette.BrokerConstants.INFLIGHT_WINDOW_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ConnectAckTest extends  AbstractServerIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectAckTest.class);
    public static final int EXPECTED_TOPIC_ALIAS_MX = 7;
    private MqttConnAckMessage connAck;

    @Override
    public String clientName() {
        return "client";
    }

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        stopServer();
        IConfig config = new FluentConfig()
            .dataPath(dbPath)
            .enablePersistence()
            .port(1883)
            .disableTelemetry()
            .persistentQueueType(FluentConfig.PersistentQueueType.SEGMENTED)
            .topicAliasMaximum(EXPECTED_TOPIC_ALIAS_MX)
            .build();
        startServer(config);

        // Reconnect the TCP
        lowLevelClient = new Client("localhost").clientId(clientName());

        connAck = lowLevelClient.connectV5();
        assertEquals(MqttConnectReturnCode.CONNECTION_ACCEPTED, connAck.variableHeader().connectReturnCode(), "Client connected");
    }

    private <T> void verifyProperty(MqttPropertyType propertyType, MqttProperties props, T expectedValue, String comment) {
        final MqttProperties.MqttProperty<Integer> property = props.getProperty(propertyType.value());
        assertEquals(expectedValue, property.value(), comment);
    }
    private void verifyNotSet(MqttPropertyType propertyType, MqttProperties props, String message) {
        assertNull(props.getProperty(propertyType.value()), message);
    }

    @Test
    public void testAckResponseProperties() {
        final MqttProperties ackProps = connAck.variableHeader().properties();
        verifyProperty(MqttPropertyType.SESSION_EXPIRY_INTERVAL, ackProps, BrokerConstants.INFINITE_SESSION_EXPIRY, "Session expiry is infinite");
        verifyNotSet(MqttPropertyType.MAXIMUM_QOS, ackProps, "Maximum QoS is not set => QoS 2 ready");
        verifyProperty(MqttPropertyType.RETAIN_AVAILABLE, ackProps, 1, "Retain feature is available");
        verifyNotSet(MqttPropertyType.MAXIMUM_PACKET_SIZE, ackProps, "Maximum packet size is the one defined by specs");
        verifyProperty(MqttPropertyType.TOPIC_ALIAS_MAXIMUM, ackProps, EXPECTED_TOPIC_ALIAS_MX, "Topic alias is available");
        verifyProperty(MqttPropertyType.WILDCARD_SUBSCRIPTION_AVAILABLE, ackProps, 1, "Wildcard subscription feature is available");
        verifyProperty(MqttPropertyType.SUBSCRIPTION_IDENTIFIER_AVAILABLE, ackProps, 1, "Subscription feature is available");
        verifyProperty(MqttPropertyType.SHARED_SUBSCRIPTION_AVAILABLE, ackProps, 1, "Shared subscription feature is available");
        verifyNotSet(MqttPropertyType.AUTHENTICATION_METHOD, ackProps, "No auth method available");
        verifyNotSet(MqttPropertyType.AUTHENTICATION_DATA, ackProps, "No auth data available");
    }

    @Test
    public void testAssignedClientIdentifier() throws InterruptedException {
        Client unnamedClient = new Client("localhost").clientId("");
        connAck = unnamedClient.connectV5();
        assertEquals(MqttConnectReturnCode.CONNECTION_ACCEPTED, connAck.variableHeader().connectReturnCode(), "Client connected");
        final MqttProperties ackProps = connAck.variableHeader().properties();
        final MqttProperties.MqttProperty<String> property = ackProps.getProperty(MqttPropertyType.ASSIGNED_CLIENT_IDENTIFIER.value());
        final int clientServerGeneratedSize = 32;
        assertEquals(clientServerGeneratedSize, property.value().length(), "Server assigned client ID");

    }
}
