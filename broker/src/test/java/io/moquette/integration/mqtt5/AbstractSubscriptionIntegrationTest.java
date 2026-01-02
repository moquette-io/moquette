package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import org.jetbrains.annotations.NotNull;

import static org.junit.jupiter.api.Assertions.assertEquals;

abstract class AbstractSubscriptionIntegrationTest extends AbstractServerIntegrationTest {

    @NotNull
    static Mqtt5BlockingClient createCleanStartClient(String clientId) {
        return createClientWithStartFlagAndClientId(true, clientId);
    }

    @NotNull
    static Mqtt5BlockingClient createNonCleanStartClient(String clientId) {
        return createClientWithStartFlagAndClientId(false, clientId);
    }

    @NotNull
    static Mqtt5BlockingClient createClientWithStartFlagAndClientId(boolean cleanStart, String clientId) {
        final Mqtt5BlockingClient client = MqttClient.builder()
            .useMqttVersion5()
            .identifier(clientId)
            .serverHost("localhost")
            .serverPort(1883)
            .buildBlocking();
        Mqtt5Connect connectRequest = Mqtt5Connect.builder()
            .cleanStart(cleanStart)
            .build();
        assertEquals(Mqtt5ConnAckReasonCode.SUCCESS, client.connect(connectRequest).getReasonCode(), clientId + " connected");
        return client;
    }
}
