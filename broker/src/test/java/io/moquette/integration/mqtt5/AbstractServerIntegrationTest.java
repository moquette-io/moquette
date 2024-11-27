package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import io.moquette.BrokerConstants;
import io.moquette.testclient.Client;
import io.netty.handler.codec.mqtt.*;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static io.moquette.integration.mqtt5.TestUtils.assertConnectionAccepted;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AbstractServerIntegrationTest extends AbstractServerIntegrationWithoutClientFixture {

    Client lowLevelClient;

    static void sleepSeconds(int secondsInterval) throws InterruptedException {
        Thread.sleep(Duration.ofSeconds(secondsInterval).toMillis());
    }

    @NotNull
    Mqtt5BlockingClient createSubscriberClient() {
        String clientId = clientName();
        return createHiveBlockingClient(clientId);
    }

    public abstract String clientName();

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        lowLevelClient = new Client("localhost").clientId(clientName());
    }

    @AfterEach
    public void tearDown() throws Exception {
        lowLevelClient.shutdownConnection();
        super.tearDown();
    }

    void connectLowLevel() throws InterruptedException {
        MqttConnAckMessage connAck = lowLevelClient.connectV5();
        assertConnectionAccepted(connAck, "Connection must be accepted");
    }

    void connectLowLevel(int keepAliveSecs) throws InterruptedException {
        MqttConnAckMessage connAck = lowLevelClient.connectV5(keepAliveSecs, BrokerConstants.INFLIGHT_WINDOW_SIZE);
        assertConnectionAccepted(connAck, "Connection must be accepted");
    }

    protected void consumesPublishesInflightWindow(int inflightWindowSize) throws InterruptedException {
        for (int i = 0; i < inflightWindowSize; i++) {
            consumePendingPublishAndAcknowledge(Integer.toString(i));
        }
    }

    protected void consumePendingPublishAndAcknowledge(String expectedPayload) throws InterruptedException {
        MqttMessage mqttMessage = lowLevelClient.receiveNextMessage(Duration.ofMillis(20000));
        assertNotNull(mqttMessage, "A message MUST be received");

        assertEquals(MqttMessageType.PUBLISH, mqttMessage.fixedHeader().messageType(), "Message received should MqttPublishMessage");
        MqttPublishMessage publish = (MqttPublishMessage) mqttMessage;
        assertEquals(expectedPayload, publish.payload().toString(StandardCharsets.UTF_8));
        int packetId = publish.variableHeader().packetId();
        assertTrue(publish.release(), "Reference of publish should be released");

        acknowledge(packetId);
    }

    protected void acknowledge(int packetId) {
        acknowledge(packetId, lowLevelClient);
    }

    protected void acknowledge(int packetId, Client client) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, false, AT_MOST_ONCE,
            false, 0);
        MqttPubAckMessage pubAck = new MqttPubAckMessage(fixedHeader, MqttMessageIdVariableHeader.from(packetId));
        client.sendMessage(pubAck);
    }
}
