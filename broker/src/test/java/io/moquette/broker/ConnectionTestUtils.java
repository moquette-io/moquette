package io.moquette.broker;

import io.moquette.spi.impl.DebugUtils;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.*;

import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

class ConnectionTestUtils {

    static void assertConnectAccepted(EmbeddedChannel channel) {
        MqttConnAckMessage connAck = channel.readOutbound();
        final MqttConnectReturnCode connAckReturnCode = connAck.variableHeader().connectReturnCode();
        assertEquals("Connect must be accepted", CONNECTION_ACCEPTED, connAckReturnCode);
    }

    static void verifyReceivePublish(EmbeddedChannel embeddedChannel, String expectedTopic, String expectedContent) {
        MqttPublishMessage receivedPublish = embeddedChannel.readOutbound();
        assertPublishIsCorrect(expectedTopic, expectedContent, receivedPublish);
    }

    private static void assertPublishIsCorrect(String expectedTopic, String expectedContent, MqttPublishMessage receivedPublish) {
        assertNotNull("Expecting a PUBLISH message", receivedPublish);
        final String decodedPayload = DebugUtils.payload2Str(receivedPublish.payload());
        assertEquals(expectedContent, decodedPayload);
        assertEquals(expectedTopic, receivedPublish.variableHeader().topicName());
    }

    static void verifyReceiveRetainedPublish(EmbeddedChannel embeddedChannel, String expectedTopic, String expectedContent) {
        MqttPublishMessage receivedPublish = embeddedChannel.readOutbound();
        assertPublishIsCorrect(expectedTopic, expectedContent, receivedPublish);
        assertTrue("MUST be retained publish", receivedPublish.fixedHeader().isRetain());
    }

    static void verifyReceiveRetainedPublish(EmbeddedChannel embeddedChannel, String expectedTopic,
                                             String expectedContent, MqttQoS expectedQos) {
        MqttPublishMessage receivedPublish = embeddedChannel.readOutbound();
        assertEquals(receivedPublish.fixedHeader().qosLevel(), expectedQos);
        assertPublishIsCorrect(expectedTopic, expectedContent, receivedPublish);
        assertTrue("MUST be retained publish", receivedPublish.fixedHeader().isRetain());
    }

    static MqttConnectMessage buildConnect(String clientId) {
        return MqttMessageBuilders.connect()
            .clientId(clientId)
            .build();
    }

    static MqttConnectMessage buildConnectNotClean(String clientId) {
        return MqttMessageBuilders.connect()
            .clientId(clientId)
            .cleanSession(false)
            .build();
    }
}
