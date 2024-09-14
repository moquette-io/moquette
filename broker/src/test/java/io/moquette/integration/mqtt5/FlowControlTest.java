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

import io.moquette.broker.config.FluentConfig;
import io.moquette.broker.config.IConfig;
import io.moquette.testclient.Client;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static io.moquette.integration.mqtt5.TestUtils.assertConnectionAccepted;
import static io.netty.handler.codec.mqtt.MqttQoS.EXACTLY_ONCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FlowControlTest extends AbstractServerIntegrationTest {

    @Test
    public void givenServerWithReceiveMaximumWhenClientPassSendQuotaThenIsDisconnected() throws IOException, InterruptedException {
        final int serverSendQuota = 5;

        // stop existing broker to restart with receiveMaximum configured
        stopServer();
        IConfig config = new FluentConfig()
            .dataPath(dbPath)
            .enablePersistence()
            .port(1883)
            .disableTelemetry()
            .persistentQueueType(FluentConfig.PersistentQueueType.SEGMENTED)
            .receiveMaximum(serverSendQuota) //configure the server to use accept a send-quota of 5
            .build();
        startServer(config);

        // Reconnect the TCP
        lowLevelClient = new Client("localhost").clientId(clientName());

        MqttConnAckMessage connAck = lowLevelClient.connectV5();
        assertConnectionAccepted(connAck, "Connection must be accepted");

        // send more PUB1 than the server's "receive-maximum", this should trigger a DISCONNECT 0x93
        for (int i = 0; i < serverSendQuota; i++) {
            sendQoS2Publish();

            verifyReceived(MqttMessageType.PUBREC);
        }

        System.out.println("Sending 6th message");
        // sixth should exceed quota and the client should get a disconnect
        sendQoS2Publish();

        MqttMessage receivedMsg = lowLevelClient.lastReceivedMessage();
        assertEquals(MqttMessageType.DISCONNECT, receivedMsg.fixedHeader().messageType(),
            "On sixth in flight message the send quota is exhausted and response should be DISCONNECT");
        MqttReasonCodeAndPropertiesVariableHeader disconnectHeader = (MqttReasonCodeAndPropertiesVariableHeader) receivedMsg.variableHeader();
        assertEquals(MqttReasonCodes.Disconnect.RECEIVE_MAXIMUM_EXCEEDED.byteValue(), disconnectHeader.reasonCode());
            // this is fragile, but have to wait that the channelInactive is propagated
        Thread.sleep(100);
        assertTrue(lowLevelClient.isConnectionLost(), "Connection MUST be closed by the server");
    }

    private void verifyReceived(MqttMessageType expectedMessageType) {
        MqttMessage receivedMsg = lowLevelClient.lastReceivedMessage();
        assertEquals(expectedMessageType, receivedMsg.fixedHeader().messageType());
    }

    private void sendQoS2Publish() {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, EXACTLY_ONCE,
            false, 0);
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader("temperature/living", 1, MqttProperties.NO_PROPERTIES);
        ByteBuf payload = Unpooled.wrappedBuffer("18°C".getBytes(StandardCharsets.UTF_8));
        MqttPublishMessage publishQoS2 = new MqttPublishMessage(fixedHeader, variableHeader, payload);
        lowLevelClient.publish(publishQoS2, 500, TimeUnit.MILLISECONDS);
    }

    @Override
    public String clientName() {
        return "sender";
    }
}
