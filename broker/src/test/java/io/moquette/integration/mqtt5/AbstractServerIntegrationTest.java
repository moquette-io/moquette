package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import io.moquette.testclient.Client;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static io.moquette.integration.mqtt5.TestUtils.assertConnectionAccepted;

public abstract class AbstractServerIntegrationTest extends AbstractServerIntegrationWithoutClientFixture {

    Client lowLevelClient;

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

    void connectLowLevel() {
        MqttConnAckMessage connAck = lowLevelClient.connectV5();
        assertConnectionAccepted(connAck, "Connection must be accepted");
    }

    void connectLowLevel(int keepAliveSecs) {
        MqttConnAckMessage connAck = lowLevelClient.connectV5(keepAliveSecs);
        assertConnectionAccepted(connAck, "Connection must be accepted");
    }
}
