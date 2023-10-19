package io.moquette.broker;

import io.moquette.broker.subscriptions.Topic;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.moquette.BrokerConstants.FLIGHT_BEFORE_RESEND_MS;
import io.moquette.broker.subscriptions.Subscription;

import java.time.Clock;
import java.util.Arrays;
import org.assertj.core.api.Assertions;

import static io.moquette.broker.Session.INFINITE_EXPIRY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static io.moquette.BrokerConstants.NO_BUFFER_FLUSH;
import static org.junit.jupiter.api.Assertions.*;

public class SessionTest {

    private static final String CLIENT_ID = "Subscriber";

    private EmbeddedChannel testChannel;
    private Session client;
    private SessionMessageQueue<SessionRegistry.EnqueuedMessage> queuedMessages;

    @BeforeEach
    public void setUp() {
        testChannel = new EmbeddedChannel();
        queuedMessages = new InMemoryQueue();
        final Clock clock = Clock.systemDefaultZone();
        final ISessionsRepository.SessionData data = new ISessionsRepository.SessionData(CLIENT_ID, MqttVersion.MQTT_3_1_1, INFINITE_EXPIRY, clock);
        client = new Session(data, true, queuedMessages);
        createConnection(client);
    }

    @Test
    public void testPubAckDrainMessagesRemainingInQueue() {
        final Topic destinationTopic = new Topic("/a/b");
        sendQoS1To(client, destinationTopic, "Hello World!");
        // simulate a filling of inflight space and start pushing on queue
        for (int i = 0; i < 10; i++) {
            sendQoS1To(client, destinationTopic, "Hello World " + i + "!");
        }

        assertFalse(queuedMessages.isEmpty(), "Inflight zone must be full, and the 11th message must be queued");
        // Exercise
        client.pubAckReceived(1);

        // Verify
        assertTrue(queuedMessages.isEmpty(), "Messages should be drained");

        // release the rest, to avoid leaking buffers
        for (int i = 2; i <= 11; i++) {
            client.pubAckReceived(i);
        }
        client.closeImmediately();
        testChannel.close();
    }

    private void sendQoS1To(Session client, Topic destinationTopic, String message) {
        final ByteBuf payload = ByteBufUtil.writeUtf8(UnpooledByteBufAllocator.DEFAULT, message);
        client.sendNotRetainedPublishOnSessionAtQos(destinationTopic, MqttQoS.AT_LEAST_ONCE, payload);
    }

    @Test
    public void testFirstResendOfANotAckedMessage() throws InterruptedException {
        final Topic destinationTopic = new Topic("/a/b");
        sendQoS1To(client, destinationTopic, "Message not ACK-ed at first send!");
        // verify the first time the message is sent
        ConnectionTestUtils.verifyReceivePublish(testChannel, destinationTopic.toString(), "Message not ACK-ed at first send!");

        // elapse the time to make the message eligible for resend
        Thread.sleep(FLIGHT_BEFORE_RESEND_MS + 1_000);

        //trigger the resend for the timeout
        client.resendInflightNotAcked();

        // verify the first time the message is sent
        ConnectionTestUtils.verifyReceivePublish(testChannel, destinationTopic.toString(), "Message not ACK-ed at first send!");
    }

    @Test
    public void testSecondResendOfANotAckedMessage() throws InterruptedException {
        final Topic destinationTopic = new Topic("/a/b");
        sendQoS1To(client, destinationTopic, "Message not ACK-ed at first send!");
        // verify the first time the message is sent
        ConnectionTestUtils.verifyReceivePublish(testChannel, destinationTopic.toString(), "Message not ACK-ed at first send!");

        // elapse the time to make the message eligible for resend
        Thread.sleep(FLIGHT_BEFORE_RESEND_MS + 1_000);

        //trigger the resend for the timeout
        client.resendInflightNotAcked();

        // verify the first time the message is sent
        ConnectionTestUtils.verifyReceivePublish(testChannel, destinationTopic.toString(), "Message not ACK-ed at first send!");

        // simulate a not ACK for the resent
        Thread.sleep(FLIGHT_BEFORE_RESEND_MS + 1_000);

        //trigger the resend for the timeout
        client.resendInflightNotAcked();

        // verify the first time the message is sent
        ConnectionTestUtils.verifyReceivePublish(testChannel, destinationTopic.toString(), "Message not ACK-ed at first send!");
    }

    @Test
    public void testRemoveSubscription() {
        client.addSubscriptions(Arrays.asList(new Subscription(CLIENT_ID, new Topic("topic/one"), MqttQoS.AT_MOST_ONCE)));
        Assertions.assertThat(client.getSubscriptions()).hasSize(1);
        client.addSubscriptions(Arrays.asList(new Subscription(CLIENT_ID, new Topic("topic/one"), MqttQoS.EXACTLY_ONCE)));
        Assertions.assertThat(client.getSubscriptions()).hasSize(1);
        client.removeSubscription(new Topic("topic/one"));
        Assertions.assertThat(client.getSubscriptions()).isEmpty();
    }

    private void createConnection(Session client) {
        BrokerConfiguration brokerConfiguration = new BrokerConfiguration(true, false, false, NO_BUFFER_FLUSH);
        MQTTConnection mqttConnection = new MQTTConnection(testChannel, brokerConfiguration, null, null, null);
        client.markConnecting();
        client.bind(mqttConnection);
        client.completeConnection();
    }
}
