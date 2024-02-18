package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.unsubscribe.unsuback.Mqtt5UnsubAck;
import com.hivemq.client.mqtt.mqtt5.message.unsubscribe.unsuback.Mqtt5UnsubAckReasonCode;
import io.moquette.broker.Server;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.broker.security.DeclarativeAuthorizatorPolicy;
import io.moquette.broker.security.IAuthorizatorPolicy;
import io.moquette.broker.subscriptions.Topic;
import io.moquette.integration.IntegrationUtils;
import io.moquette.testclient.Client;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttReasonCodes;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SharedSubscriptionTest extends AbstractSubscriptionIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(SharedSubscriptionTest.class);

    @Override
    public String clientName() {
        return "subscriber";
    }

    @Test
    public void givenAClientSendingBadlyFormattedSharedSubscriptionNameThenItIsDisconnected() {
        connectLowLevel();

        MqttMessage received = lowLevelClient.subscribeWithError("$share/+/measures/temp", MqttQoS.AT_LEAST_ONCE);

        // verify received is a disconnect with an error
        verifyOfType(received, MqttMessageType.DISCONNECT);
        MqttReasonCodeAndPropertiesVariableHeader disconnectHeader = (MqttReasonCodeAndPropertiesVariableHeader) received.variableHeader();
        assertEquals(MqttReasonCodes.Disconnect.MALFORMED_PACKET.byteValue(), disconnectHeader.reasonCode());
    }

    @Test
    public void givenClientSubscribingToSharedTopicThenReceiveTheExpectedSubscriptionACK() {
        connectLowLevel();

        MqttMessage received = lowLevelClient.subscribeWithError("$share/metrics/measures/temp", MqttQoS.AT_LEAST_ONCE);

        verifyOfType(received, MqttMessageType.SUBACK);
        MqttSubAckMessage subAckMessage = (MqttSubAckMessage) received;
        List<Integer> grantedQoSes = subAckMessage.payload().grantedQoSLevels();
        assertEquals(1, grantedQoSes.size(), "Granted qos list must be the same cardinality of the subscribe request");
        assertEquals(MqttQoS.AT_LEAST_ONCE.value(), grantedQoSes.iterator().next());
    }

    @Test
    public void givenATopicNotReadableWhenAClientSubscribeSharedThenReceiveSubackWithNegativeResponse() throws IOException {
        // stop already started broker instance
        stopServer();

        final IAuthorizatorPolicy policy = new DeclarativeAuthorizatorPolicy.Builder().build();
        startServer(dbPath, policy);

        // Connect the client to newly started broker
        lowLevelClient = new Client("localhost").clientId(clientName());

        connectLowLevel();

        MqttSubAckMessage subAckMessage = lowLevelClient.subscribe("$share/metrics/measures/temp", MqttQoS.AT_LEAST_ONCE);

        List<Integer> grantedQoSes = subAckMessage.payload().grantedQoSLevels();
        assertEquals(1, grantedQoSes.size(),
            "Granted qos list must be the same cardinality of the subscribe request");
        assertEquals(MqttQoS.FAILURE.value(), grantedQoSes.iterator().next(),
            "Not readable topic should reflect also in shared subscription");
    }


    @Test
    public void givenClientSubscribingToSharedAndNonSharedWhenTheSharedIsNotReadableReceivesPositiveAckOnlyForNonShared() throws IOException {
        // stop already started broker instance
        stopServer();

        final String clientId = clientName();
        final IAuthorizatorPolicy policy = new DeclarativeAuthorizatorPolicy.Builder()
            .readFrom(Topic.asTopic("/sensors/living/temp"), null, clientId)
            .build();
        startServer(dbPath, policy);

        // Connect the client to newly started broker
        lowLevelClient = new Client("localhost").clientId(clientId);

        connectLowLevel();

        MqttSubAckMessage subAckMessage = lowLevelClient.subscribe(
            "/sensors/living/temp", MqttQoS.EXACTLY_ONCE,
            "$share/metrics/measures/temp", MqttQoS.AT_LEAST_ONCE);

        List<Integer> grantedQoSes = subAckMessage.payload().grantedQoSLevels();
        assertEquals(2, grantedQoSes.size(),
            "Granted qos list must be the same cardinality of the subscribe request");
        Iterator<Integer> replyQoSes = grantedQoSes.iterator();
        assertEquals(MqttQoS.EXACTLY_ONCE.value(), replyQoSes.next(),
            "Non shared readable subscription must be accepted");
        assertEquals(MqttQoS.FAILURE.value(), replyQoSes.next(),
            "Not readable topic should reflect also in shared subscription");
    }

    protected void startServer(String dbPath, IAuthorizatorPolicy authPolicy) throws IOException {
        broker = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
        config = new MemoryConfig(configProps);
        broker.startServer(config, null, null, null, authPolicy);
    }

    @Test
    public void givenASharedSubscriptionClientReceivesANotification() throws Exception {
        final Mqtt5BlockingClient subscriberClient = createSubscriberClient();
        subscriberClient.subscribeWith()
            .topicFilter("$share/collectors/metric/temperature/#")
            .send();

        Mqtt5BlockingClient publisherClient = createPublisherClient();

        verifyPublishedMessage(subscriberClient, unused -> publisherClient.publishWith()
            .topic("metric/temperature/living")
            .payload("18".getBytes(StandardCharsets.UTF_8))
            .send(), MqttQos.AT_MOST_ONCE, "18", "Shared message must be received", 10);
    }

    @Test
    public void givenAClientWithOverlappingSharedSubscriptionsThenReceivesMultiplePublishes() throws InterruptedException {
        // Connect a subscriber client
        lowLevelClient = new Client("localhost").clientId(clientName());

        connectLowLevel();

        // subscribe to a shared topic
        MqttSubAckMessage subAckMessage = lowLevelClient.subscribe(
            "$share/collectors/metric/temperature/#", MqttQoS.AT_MOST_ONCE,
            "$share/thermo_living/metric/temperature/living", MqttQoS.AT_MOST_ONCE);

        List<Integer> grantedQoSes = subAckMessage.payload().grantedQoSLevels();
        assertEquals(2, grantedQoSes.size(),
            "Granted qos list must be the same cardinality of the subscribe request");
        assertEquals(MqttQoS.AT_MOST_ONCE.value(), grantedQoSes.iterator().next(),
            "Client is subscribed to the shared topic");
        assertEquals(MqttQoS.AT_MOST_ONCE.value(), grantedQoSes.iterator().next(),
            "Client is subscribed to the shared topic");

        Mqtt5BlockingClient publisherClient = createPublisherClient();
        publish(publisherClient, "metric/temperature/living", MqttQos.AT_MOST_ONCE);

        MqttMessage received = lowLevelClient.receiveNextMessage(Duration.ofSeconds(1));
        verifyPubPayload(received, "18");
        received = lowLevelClient.receiveNextMessage(Duration.ofSeconds(1));
        verifyPubPayload(received, "18");
    }

    private static void verifyPubPayload(MqttMessage received, String expectedPayload) {
        assertNotNull(received);
        assertEquals(MqttPublishMessage.class, received.getClass());
        MqttPublishMessage pub = (MqttPublishMessage) received;
        String payload = pub.payload().asByteBuf().toString(StandardCharsets.UTF_8);
        assertEquals(expectedPayload, payload);
        assertTrue(pub.release(), "received message must be deallocated");
    }

    @Test
    public void whenAClientSubscribeToASharedTopicThenDoesntReceiveAnyRetainedMessagedOnTheMatchingTopicFilter() throws InterruptedException {
        // publish a message with retained on a shared topic
        Mqtt5BlockingClient publisherClient = createPublisherClient();
        publisherClient.publishWith()
            .topic("temperature/living")
            .payload("18".getBytes(StandardCharsets.UTF_8))
            .qos(MqttQos.AT_LEAST_ONCE) // Broker retains only QoS1 and QoS2
            .retain(true)
            .send();

        // Connect a subscriber client
        lowLevelClient = new Client("localhost").clientId(clientName());

        connectLowLevel();

        // subscribe to a shared topic
        MqttSubAckMessage subAckMessage = lowLevelClient.subscribe(
            "$share/collectors/temperature/#", MqttQoS.AT_LEAST_ONCE);

        List<Integer> grantedQoSes = subAckMessage.payload().grantedQoSLevels();
        assertEquals(1, grantedQoSes.size(),
            "Granted qos list must be the same cardinality of the subscribe request");
        assertEquals(MqttQoS.AT_LEAST_ONCE.value(), grantedQoSes.iterator().next(),
            "Client is subscribed to the shared topic");

        MqttMessage received = lowLevelClient.receiveNextMessage(Duration.ofSeconds(1));
        assertNull(received, "No retained messages MUST be published");
    }

    @Test
    public void givenSharedSubscriptionWithCertainQoSWhenSameClientWithSameShareSubscribeToSameTopicFilterThenQoSUpdates() throws Exception {
        final Mqtt5BlockingClient subscriberClient = createSubscriberClient();
        subscribe(subscriberClient, "$share/collectors/metric/temperature/living", MqttQos.AT_MOST_ONCE);

        Mqtt5BlockingClient publisherClient = createPublisherClient();

        verifyPublishedMessage(subscriberClient,
            unused -> publish(publisherClient, "metric/temperature/living", MqttQos.AT_LEAST_ONCE),
            MqttQos.AT_MOST_ONCE, "18", "QoS0 publish message is expected by the subscriber when subscribed with AT_MOST_ONCE", 1);

        // update QoS for shared subscription
        subscribe(subscriberClient, "$share/collectors/metric/temperature/living", MqttQos.AT_LEAST_ONCE);

        // This time the publish reaches the subscription
        verifyPublishedMessage(subscriberClient, v -> {
            // publish the message again and verify the captured message
            publish(publisherClient, "metric/temperature/living", MqttQos.AT_LEAST_ONCE);
        }, MqttQos.AT_LEAST_ONCE, "18", "Shared message must be received", 30);
    }

    private static void publish(Mqtt5BlockingClient publisherClient, String topicName, MqttQos mqttQos) {
        publisherClient.publishWith()
            .topic(topicName)
            .qos(mqttQos)
            .payload("18".getBytes(StandardCharsets.UTF_8))
            .send();
    }

    static void subscribe(Mqtt5BlockingClient subscriberClient, String topicFilter, MqttQos mqttQos) {
        subscriberClient.subscribeWith()
            .topicFilter(topicFilter)
            .qos(mqttQos)
            .send();
    }

    @Test
    public void givenMultipleClientSubscribedToSharedSubscriptionWhenOneUnsubscribeThenTheSharedSubscriptionRemainsValid() throws Exception {
        String fullSharedSubscriptionTopicFilter = "$share/collectors/metric/temperature/living";

        // subscribe first client to shared subscription
        final Mqtt5BlockingClient subscriber1 = createSubscriberClient("subscriber1");
        subscribe(subscriber1, fullSharedSubscriptionTopicFilter, MqttQos.AT_LEAST_ONCE);

        // subscribe second client to shared subscription
        final Mqtt5BlockingClient subscriber2 = createSubscriberClient("subscriber2");
        subscribe(subscriber2, fullSharedSubscriptionTopicFilter, MqttQos.AT_LEAST_ONCE);

        // unsubscribe successfully the first subscriber
        Mqtt5UnsubAck result = subscriber1.unsubscribeWith()
            .topicFilter(fullSharedSubscriptionTopicFilter)
            .send();
        assertTrue(result.getReasonCodes().stream().allMatch(rc -> rc == Mqtt5UnsubAckReasonCode.SUCCESS),
            "Unsubscribe of shared subscription must be successful");


        // verify it's received from the survivor subscriber2
        Mqtt5BlockingClient publisherClient = createPublisherClient();
        // try 4 times we should hit all the 4 times the subscriber2
        // if the other shared subscription remains active we have 50% of possibility
        // to hit the not removed subscriber, so 4 iterations should be enough.
        for (int i = 0; i < 4; i++) {
            verifyPublishedMessage(subscriber2, v -> {
                // push a message to the shared subscription
                publish(publisherClient, "metric/temperature/living", MqttQos.AT_LEAST_ONCE);
            }, MqttQos.AT_LEAST_ONCE, "18", "Shared message must be received", 2);
        }
    }

    @Test
    public void givenASharedSubscriptionWhenLastSubscribedClientUnsubscribeThenTheSharedSubscriptionCeasesToExist() throws Exception {
        String fullSharedSubscriptionTopicFilter = "$share/collectors/metric/temperature/living";

        // subscribe client to shared subscription
        final Mqtt5BlockingClient subscriber = createSubscriberClient("subscriber1");
        subscribe(subscriber, fullSharedSubscriptionTopicFilter, MqttQos.AT_LEAST_ONCE);

        // verify subscribed to the shared receives a message
        Mqtt5BlockingClient publisherClient = createPublisherClient();
        verifyPublishedMessage(subscriber, v -> {
            // push a message to the shared subscription
            publish(publisherClient, "metric/temperature/living", MqttQos.AT_LEAST_ONCE);
        }, MqttQos.AT_LEAST_ONCE, "18", "Shared message must be received", 2);

        // unsubscribe the only shared subscription client
        Mqtt5UnsubAck result = subscriber.unsubscribeWith()
            .topicFilter(fullSharedSubscriptionTopicFilter)
            .send();
        assertTrue(result.getReasonCodes().stream().allMatch(rc -> rc == Mqtt5UnsubAckReasonCode.SUCCESS),
            "Unsubscribe of shared subscription must be successful");

        // verify no publish is propagated by shared subscription
        verifyNoPublish(subscriber, v -> {
                // push a message to the shared subscription
                publish(publisherClient, "metric/temperature/living", MqttQos.AT_LEAST_ONCE);
            }, Duration.ofSeconds(2),
            "Subscriber must not receive any message from the left shared subscription");
    }

    @Test
    public void givenASharedSubscriptionWhenLastSubscribedClientSessionTerminatesThenTheSharedSubscriptionCeasesToExist() throws Exception {
        String fullSharedSubscriptionTopicFilter = "$share/collectors/metric/temperature/living";

        // subscribe client to shared subscription
        final Mqtt5BlockingClient subscriber = createCleanStartClient("subscriber1");
        subscribe(subscriber, fullSharedSubscriptionTopicFilter, MqttQos.AT_LEAST_ONCE);

        // verify subscribed to the shared receives a message
        Mqtt5BlockingClient publisherClient = createPublisherClient();
        verifyPublishedMessage(subscriber, v -> {
            // push a message to the shared subscription
            publish(publisherClient, "metric/temperature/living", MqttQos.AT_LEAST_ONCE);
        }, MqttQos.AT_LEAST_ONCE, "18", "Shared message must be received", 2);

        // disconnect the subscriber, so becuase it's clean, wipe all shared subscriptions
        subscriber.disconnect();

        // verify that a publish on shared topic doesn't have any side effect
        verifyNoPublish(subscriber, v -> {
            // push a message to the shared subscription
            publish(publisherClient, "metric/temperature/living", MqttQos.AT_LEAST_ONCE);
        }, Duration.ofSeconds(2), "Shared message must be received");
    }

    @Test
    public void givenASharedSubscriptionWhenBrokerRestartsAndClientReconnectsThenSharedSubscriptionIsReloaded() throws Exception {
        String fullSharedSubscriptionTopicFilter = "$share/collectors/metric/temperature/living";

        // subscribe client to shared subscription
        Mqtt5BlockingClient subscriber = createNonCleanStartClient("subscriber");
        subscribe(subscriber, fullSharedSubscriptionTopicFilter, MqttQos.AT_LEAST_ONCE);

        // verify subscribed to the shared receives a message
        final Mqtt5BlockingClient publisherClient = createPublisherClient();
        verifyPublishedMessage(subscriber, v -> {
            // push a message to the shared subscription
            publish(publisherClient, "metric/temperature/living", MqttQos.AT_LEAST_ONCE);
        }, MqttQos.AT_LEAST_ONCE, "18", "Shared message must be received", 2);

        // restart the broker
        restartServerWithSuspension(Duration.ofSeconds(2));

        // reconnect subscriber
        subscriber = createNonCleanStartClient("subscriber");

        // verify after restart the shared subscription becomes again active
        final Mqtt5BlockingClient publisherClientReconnected = createPublisherClient();
        verifyPublishedMessage(subscriber, v -> {
            // push a message to the shared subscription
            publish(publisherClientReconnected, "metric/temperature/living", MqttQos.AT_LEAST_ONCE);
        }, MqttQos.AT_LEAST_ONCE, "18", "Shared message must be received", 2);
    }
}
