/*
 * Copyright (c) 2012-2016 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package io.moquette.metrics.prometheus;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * Used in test to collect all messages received asynchronously by MqttClient.
 */
public class MessageCollector implements MqttCallback {

    private static final class ReceivedMessage {

        private final MqttMessage message;
        private final String topic;

        private ReceivedMessage(MqttMessage message, String topic) {
            this.message = message;
            this.topic = topic;
        }
    }

    private BlockingQueue<ReceivedMessage> m_messages = new LinkedBlockingQueue<>();
    private boolean m_connectionLost;
    private volatile boolean messageReceived = false;

    /**
     * Return the message from the queue if not empty, else return null with
     * wait period.
     */
    public MqttMessage getMessageImmediate() {
        if (m_messages.isEmpty()) {
            return null;
        }
        try {
            return m_messages.take().message;
        } catch (InterruptedException e) {
            return null;
        }
    }

    public MqttMessage retrieveMessage() throws InterruptedException {
        final ReceivedMessage content = m_messages.take();
        messageReceived = false;
        return content.message;
    }

    public String retrieveTopic() throws InterruptedException {
        final ReceivedMessage content = m_messages.take();
        messageReceived = false;
        return content.topic;
    }

    public boolean isMessageReceived() {
        return messageReceived;
    }

    void reinit() {
        m_messages = new LinkedBlockingQueue<>();
        m_connectionLost = false;
        messageReceived = false;
    }

    public boolean connectionLost() {
        return m_connectionLost;
    }

    @Override
    public void connectionLost(Throwable cause) {
        m_connectionLost = true;
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        m_messages.offer(new ReceivedMessage(message, topic));
        messageReceived = true;
    }

    /**
     * Invoked when the message sent to a integration is ACKED (PUBACK or
     * PUBCOMP by the integration)
     */
    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
//        try {
//            token.waitForCompletion(1_000);
//            m_messages.offer(new ReceivedMessage(token.getMessage(), token.getTopics()[0]));
//        } catch (MqttException e) {
//            e.printStackTrace();
//        }
    }
}
