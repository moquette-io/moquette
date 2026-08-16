package io.moquette.integration;

import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.testclient.Client;
import io.netty.handler.codec.mqtt.*;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies broker behaviour when a second CONNECT is sent on an already-established channel.
 *
 * MQTT 3.1.1 §3.1 [MQTT-3.1.0-2]: the server MUST treat it as a protocol violation and disconnect the client.
 * MQTT 5.0   §3.1.4 [MQTT-3.1.0-2]: it is a Protocol Error; the broker must send DISCONNECT (0x82) then close.
 */
public class DoubleConnectOnSameChannelTest {

    Server broker;
    Client lowLevelClient;
    IConfig config;

    @TempDir
    Path tempFolder;

    @BeforeAll
    public static void beforeTests() {
        Awaitility.setDefaultTimeout(Durations.TWO_SECONDS);
    }

    @BeforeEach
    public void setUp() throws IOException {
        String dbPath = IntegrationUtils.tempH2Path(tempFolder);
        broker = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
        config = new MemoryConfig(configProps);
        broker.startServer(config);
        lowLevelClient = new Client("localhost");
    }

    @AfterEach
    public void tearDown() throws InterruptedException {
        lowLevelClient.shutdownConnection();
        Thread.sleep(300);
        broker.stopServer();
    }

    // [MQTT-3.1.0-2] MQTT 3.1.1 §3.1
    @Test
    public void givenConnectedMqtt311ClientWhenASecondConnectIsSentOnSameChannelThenChannelIsForciblyClose()
        throws InterruptedException {
        lowLevelClient.sendMessage(buildConnect311("client1"));

        MqttMessage firstResponse = lowLevelClient.receiveNextMessage(Duration.ofSeconds(2));
        assertNotNull(firstResponse, "First CONNECT must receive a CONNACK");
        assertTrue(firstResponse instanceof MqttConnAckMessage, "Response to first CONNECT must be CONNACK");
        assertEquals(MqttConnectReturnCode.CONNECTION_ACCEPTED,
            ((MqttConnAckMessage) firstResponse).variableHeader().connectReturnCode(),
            "First CONNECT must be accepted");

        // second CONNECT on the same already-connected channel
        lowLevelClient.sendMessage(buildConnect311("client2"));

        Awaitility.await("Channel must be closed after second CONNECT [MQTT-3.1.0-2]")
            .atMost(2, TimeUnit.SECONDS)
            .until(lowLevelClient::isConnectionLost);
    }

    // [MQTT-3.1.0-2] MQTT 5.0 §3.1.4
    @Test
    public void givenConnectedMqtt5ClientWhenASecondConnectIsSentOnSameChannelThenBrokerSendsDisconnectProtocolErrorAndClosesChannel()
        throws InterruptedException {
        lowLevelClient.sendMessage(buildConnect5("client1"));

        MqttMessage firstResponse = lowLevelClient.receiveNextMessage(Duration.ofSeconds(2));
        assertNotNull(firstResponse, "First CONNECT must receive a CONNACK");
        assertTrue(firstResponse instanceof MqttConnAckMessage, "Response to first CONNECT must be CONNACK");
        assertEquals(MqttConnectReturnCode.CONNECTION_ACCEPTED,
            ((MqttConnAckMessage) firstResponse).variableHeader().connectReturnCode(),
            "First CONNECT must be accepted");

        // second CONNECT on the same already-connected channel
        lowLevelClient.sendMessage(buildConnect5("client2"));

        // broker must send DISCONNECT with reason code Protocol Error (0x82) before closing
        MqttMessage brokerResponse = lowLevelClient.receiveNextMessage(Duration.ofSeconds(2));
        assertNotNull(brokerResponse, "Broker must send CONNACK after second CONNECT with specific error code");
        assertEquals(MqttMessageType.CONNACK, brokerResponse.fixedHeader().messageType(),
            "Broker response to second CONNECT must be CONNACK");
        MqttConnAckVariableHeader connAckHeader =
            (MqttConnAckVariableHeader) brokerResponse.variableHeader();
        assertEquals(MqttConnectReturnCode.CONNECTION_REFUSED_PROTOCOL_ERROR, connAckHeader.connectReturnCode(),
            "CONNACK reason code must be Protocol Error (0x82)");

        Awaitility.await("Channel must be closed after CONNACK [MQTT-3.1.0-2]")
            .atMost(2, TimeUnit.SECONDS)
            .until(lowLevelClient::isConnectionLost);
    }

    private static MqttConnectMessage buildConnect311(String clientId) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
            MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader varHeader = new MqttConnectVariableHeader(
            MqttVersion.MQTT_3_1_1.protocolName(), MqttVersion.MQTT_3_1_1.protocolLevel(),
            false, false, false, 0, false, true, 60);
        MqttConnectPayload payload = new MqttConnectPayload(clientId, null, null, null, (byte[]) null);
        return new MqttConnectMessage(fixedHeader, varHeader, payload);
    }

    private static MqttConnectMessage buildConnect5(String clientId) {
        return MqttMessageBuilders.connect()
            .protocolVersion(MqttVersion.MQTT_5)
            .clientId(clientId)
            .keepAlive(60)
            .build();
    }
}
