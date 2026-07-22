package io.moquette.integration;

import io.moquette.BrokerConstants;
import io.moquette.integration.mqtt5.AbstractServerIntegrationTest;
import io.moquette.testclient.Client;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

public class SubscribeTest extends AbstractServerIntegrationTest {

    @Override
    public String clientName() {
        return "Subscriber";
    }

    @Test
    public void givenAConnectedClientWhenItSubscribesWithTooDeepTopicFilterConnectionIsDropped() throws InterruptedException {
        // given a connected low-level client
        Client client = new Client("localhost").clientId("deep-sub-client");
        MqttConnAckMessage connAck = client.connectV5();
        io.moquette.integration.mqtt5.TestUtils.assertConnectionAccepted(connAck, "Connection must be accepted");

        // build a topic filter with MAX_TOPIC_DEPTH + 1 levels
        String deepTopic = String.join("/", Collections.nCopies(BrokerConstants.MAX_TOPIC_DEPTH + 1, "a"));

        MqttSubscribeMessage subscribe = MqttMessageBuilders.subscribe()
            .messageId(1)
            .addSubscription(MqttQoS.AT_MOST_ONCE, deepTopic)
            .build();

        // when subscribing with an over-depth topic filter
        client.sendMessage(subscribe);

        // then the broker drops the connection
        Awaitility.await()
            .atMost(Duration.ofSeconds(2))
            .until(client::isConnectionLost);

        client.shutdownConnection();
    }
}
