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

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.hivemq.client.mqtt.mqtt5.message.publish.puback.Mqtt5PubAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAckReasonCode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestResponseTest extends AbstractServerIntegrationWithoutClientFixture {

    @Test
    public void givenRequestResponseProtocolWhenRequestIsIssueThenTheResponderReply() throws InterruptedException {
        final Mqtt5BlockingClient requester = createHiveBlockingClient("requester");
        final String responseTopic = "requester/door/open/result";
        subscribeToResponseTopic(requester, responseTopic);

        final Mqtt5BlockingClient responder = createHiveBlockingClient("responder");

        Mqtt5Subscribe subscribeToRequest = Mqtt5Subscribe.builder()
            .topicFilter("requester/door/open")
            .qos(MqttQos.AT_LEAST_ONCE)
            .build();
        responder.toAsync().subscribe(subscribeToRequest,
            (Mqtt5Publish pub) -> {
                assertTrue(pub.getResponseTopic().isPresent(), "Response topic MUST defined in request publish");
                Mqtt5PublishResult.Mqtt5Qos1Result responseResult = (Mqtt5PublishResult.Mqtt5Qos1Result)responder.publishWith()
                    .topic(pub.getResponseTopic().get())
                    .payload("OK".getBytes(StandardCharsets.UTF_8))
                    .send();
                assertEquals(Mqtt5PubAckReasonCode.SUCCESS, responseResult.getPubAck().getReasonCode(),
                    "Open door response cannot be published ");
            });

        Mqtt5PublishResult.Mqtt5Qos1Result requestResult = (Mqtt5PublishResult.Mqtt5Qos1Result) requester.publishWith()
            .topic("requester/door/open")
            .responseTopic(responseTopic)
            .payload("Please open the door".getBytes(StandardCharsets.UTF_8))
            .qos(MqttQos.AT_LEAST_ONCE)
            .send();
        assertEquals(Mqtt5PubAckReasonCode.SUCCESS, requestResult.getPubAck().getReasonCode(),
            "Open door request cannot be published ");

        verifyPublishMessage(requester, msgPub -> {
            assertTrue(msgPub.getPayload().isPresent(), "Response payload MUST be present");
            String payload = new String(msgPub.getPayloadAsBytes(), StandardCharsets.UTF_8);
            assertEquals("OK", payload);
        });
    }

    private static void subscribeToResponseTopic(Mqtt5BlockingClient requester, String responseTopic) {
        Mqtt5SubAck subAck = requester.subscribeWith()
            .topicFilter(responseTopic)
            .qos(MqttQos.AT_LEAST_ONCE)
            .send();
        assertThat(subAck.getReasonCodes()).contains(Mqtt5SubAckReasonCode.GRANTED_QOS_1);
    }
}
