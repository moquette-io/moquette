/*
 *
 *  * Copyright (c) 2012-2024 The original author or authors
 *  * ------------------------------------------------------
 *  * All rights reserved. This program and the accompanying materials
 *  * are made available under the terms of the Eclipse Public License v1.0
 *  * and Apache License v2.0 which accompanies this distribution.
 *  *
 *  * The Eclipse Public License is available at
 *  * http://www.eclipse.org/legal/epl-v10.html
 *  *
 *  * The Apache License v2.0 is available at
 *  * http://www.opensource.org/licenses/apache2.0.php
 *  *
 *  * You may elect to redistribute this code under either of these licenses.
 *
 */

package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.hivemq.client.mqtt.mqtt5.message.publish.puback.Mqtt5PubAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAckReasonCode;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class RequestResponseTest extends AbstractServerIntegrationWithoutClientFixture {

    @Test
    public void givenRequestResponseProtocolWhenRequestIsIssueThenTheResponderReply() throws InterruptedException {
        final Mqtt5BlockingClient requester = createHiveBlockingClient("requester");
        final String responseTopic = "requester/door/open/result";
        subscribeToResponseTopic(requester, responseTopic);

        final Mqtt5BlockingClient responder = createHiveBlockingClient("responder");

        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = requester.publishes(MqttGlobalPublishFilter.ALL)) {
            responderRepliesToRequesterPublish(responder, requester, responseTopic);

            verifyPublishMessage(publishes, msgPub -> {
                assertTrue(msgPub.getPayload().isPresent(), "Response payload MUST be present");
                String payload = new String(msgPub.getPayloadAsBytes(), StandardCharsets.UTF_8);
                assertEquals("OK", payload);
            });
        }
    }

    private static void responderRepliesToRequesterPublish(Mqtt5BlockingClient responder, Mqtt5BlockingClient requester, String responseTopic) {
        Mqtt5Subscribe subscribeToRequest = Mqtt5Subscribe.builder()
            .topicFilter("requester/door/open")
            .qos(MqttQos.AT_LEAST_ONCE)
            .build();
        CompletableFuture<@NotNull Mqtt5SubAck> subackFuture = responder.toAsync().subscribe(subscribeToRequest,
            (Mqtt5Publish pub) -> {
                assertTrue(pub.getResponseTopic().isPresent(), "Response topic MUST defined in request publish");
                Mqtt5PublishResult responseResult = responder.publishWith()
                    .topic(pub.getResponseTopic().get())
                    .payload("OK".getBytes(StandardCharsets.UTF_8))
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .send();
                AbstractServerIntegrationWithoutClientFixture.verifyPublishSucceeded(responseResult);
            });

        // wait for the SUBACK in 1 second, else if PUB is sent before the client is fully subscribed, then it's lost
        waitForSubAck(subackFuture);

        Mqtt5PublishResult.Mqtt5Qos1Result requestResult = (Mqtt5PublishResult.Mqtt5Qos1Result) requester.publishWith()
            .topic("requester/door/open")
            .responseTopic(responseTopic)
            .payload("Please open the door".getBytes(StandardCharsets.UTF_8))
            .qos(MqttQos.AT_LEAST_ONCE)
            .send();
        assertEquals(Mqtt5PubAckReasonCode.SUCCESS, requestResult.getPubAck().getReasonCode(),
            "Open door request cannot be published ");
    }

    private static void subscribeToResponseTopic(Mqtt5BlockingClient requester, String responseTopic) {
        subscribeToAtQos1(requester, responseTopic);
    }

    @Test
    public void givenRequestResponseProtocolWhenRequestIsIssueThenTheResponderReplyWithCorrelationData() throws InterruptedException {
        final Mqtt5BlockingClient requester = createHiveBlockingClient("requester");
        final String responseTopic = "requester/door/open/result";
        subscribeToResponseTopic(requester, responseTopic);

        final Mqtt5BlockingClient responder = createHiveBlockingClient("responder");

        Mqtt5Subscribe subscribeToRequest = Mqtt5Subscribe.builder()
            .topicFilter("requester/door/open")
            .qos(MqttQos.AT_LEAST_ONCE)
            .build();
        CompletableFuture<@NotNull Mqtt5SubAck> subackFuture = responder.toAsync().subscribe(subscribeToRequest,
            (Mqtt5Publish pub) -> {
                assertTrue(pub.getResponseTopic().isPresent(), "Response topic MUST defined in request publish");
                assertTrue(pub.getCorrelationData().isPresent(), "Correlation data MUST defined in request publish");
                Mqtt5PublishResult responseResult = responder.publishWith()
                    .topic(pub.getResponseTopic().get())
                    .correlationData(pub.getCorrelationData().get())
                    .payload("OK".getBytes(StandardCharsets.UTF_8))
                    .send();
                assertFalse(responseResult.getError().isPresent(), "Open door response cannot be published ");
            });
        waitForSubAck(subackFuture);

        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = requester.publishes(MqttGlobalPublishFilter.ALL)) {
            Mqtt5PublishResult.Mqtt5Qos1Result requestResult = (Mqtt5PublishResult.Mqtt5Qos1Result) requester.publishWith()
                .topic("requester/door/open")
                .responseTopic(responseTopic)
                .correlationData("req-open-door".getBytes(StandardCharsets.UTF_8))
                .payload("Please open the door".getBytes(StandardCharsets.UTF_8))
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
            assertEquals(Mqtt5PubAckReasonCode.SUCCESS, requestResult.getPubAck().getReasonCode(),
                "Open door request cannot be published ");

            verifyPublishMessage(publishes, msgPub -> {
                assertTrue(msgPub.getPayload().isPresent(), "Response payload MUST be present");
                String payload = new String(msgPub.getPayloadAsBytes(), StandardCharsets.UTF_8);
                assertEquals("OK", payload);
                assertTrue(msgPub.getCorrelationData().isPresent(), "Request correlation data MUST defined in response publish");
                final byte[] correlationData = asByteArray(msgPub.getCorrelationData().get());
                assertEquals("req-open-door", new String(correlationData, StandardCharsets.UTF_8));
            });
        }
    }

    private static void waitForSubAck(CompletableFuture<@NotNull Mqtt5SubAck> subackFuture) {
        try {
            Mqtt5SubAck mqtt5SubAck = subackFuture.get(1, TimeUnit.SECONDS);
            assertEquals(1, mqtt5SubAck.getReasonCodes().size());
            assertEquals(Mqtt5SubAckReasonCode.GRANTED_QOS_1, mqtt5SubAck.getReasonCodes().iterator().next());
        } catch (InterruptedException e) {
            fail("Sub ack waiting interrupted before 1 sec expires");
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            fail("Sub ack didn't arrive in 1 second timeout");
        }
    }

    private byte[] asByteArray(ByteBuffer byteBuffer) {
        byte[] arr = new byte[byteBuffer.remaining()];
        byteBuffer.get(arr);
        return arr;
    }

    @Test
    public void givenRequestResponseProtocolAndClientIsConnectedWhenRequestIsIssueThenTheResponderReply() throws InterruptedException {
        final Mqtt5BlockingClient requester = createHiveBlockingClientWithResponseProtocol("requester");
        final String responseTopic = "/reqresp/response/requester";
        subscribeToResponseTopic(requester, responseTopic);

        final Mqtt5BlockingClient responder = createHiveBlockingClient("responder");

        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = requester.publishes(MqttGlobalPublishFilter.ALL)) {
            responderRepliesToRequesterPublish(responder, requester, responseTopic);

            verifyPublishMessage(publishes, msgPub -> {
                assertTrue(msgPub.getPayload().isPresent(), "Response payload MUST be present");
                String payload = new String(msgPub.getPayloadAsBytes(), StandardCharsets.UTF_8);
                assertEquals("OK", payload);
            });
        }
    }
}
