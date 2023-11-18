package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttReasonCodes;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import static io.moquette.integration.mqtt5.ConnectTest.assertConnectionAccepted;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SharedSubscriptionTest extends AbstractServerIntegrationTest {

    @Override
    public String clientName() {
        return "subscriber";
    }

    @Test
    public void givenAClientSendingBadlyFormattedSharedSubscriptionNameThenItIsDisconnected() {
        MqttConnAckMessage connAck = lowLevelClient.connectV5();
        assertConnectionAccepted(connAck, "Connection must be accepted");

        MqttMessage received = lowLevelClient.subscribeWithError("$share/+/measures/temp", MqttQoS.AT_LEAST_ONCE);

        // verify received is a disconnect with an error
        MqttReasonCodeAndPropertiesVariableHeader disconnectHeader = (MqttReasonCodeAndPropertiesVariableHeader) received.variableHeader();
        assertEquals(MqttReasonCodes.Disconnect.MALFORMED_PACKET.byteValue(), disconnectHeader.reasonCode());
    }

    @NotNull
    private Mqtt5BlockingClient createSubscriberClient() {
        final Mqtt5BlockingClient client = MqttClient.builder()
            .useMqttVersion5()
            .identifier(clientName())
            .serverHost("localhost")
            .serverPort(1883)
            .buildBlocking();
        assertEquals(Mqtt5ConnAckReasonCode.SUCCESS, client.connect().getReasonCode(), "Subscriber connected");
        return client;
    }
}
