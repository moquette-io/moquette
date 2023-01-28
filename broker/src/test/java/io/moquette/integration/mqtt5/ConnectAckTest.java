package io.moquette.integration.mqtt5;

import io.moquette.testclient.Client;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttProperties;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static io.moquette.BrokerConstants.INFLIGHT_WINDOW_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class ConnectAckTest extends  AbstractServerIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectAckTest.class);

    @Override
    String clientName() {
        return "client";
    }

    @Test
    public void testReceiveMaximum() {
        final MqttConnAckMessage connAck = lowLevelClient.connectV5();
        assertEquals(MqttConnectReturnCode.CONNECTION_ACCEPTED, connAck.variableHeader().connectReturnCode(), "Client connected");

        final MqttProperties ackProps = connAck.variableHeader().properties();
        final MqttProperties.MqttProperty<Integer> property = ackProps.getProperty(MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM.value());
        assertEquals(INFLIGHT_WINDOW_SIZE, property.value(), "Receive maximum property must equals flight window size.");
    }
}
