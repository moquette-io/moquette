/*
 * Copyright (c) 2012-2017 The original author or authors
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

package io.moquette.server;

import io.moquette.connections.IConnectionsManager;
import io.moquette.connections.MqttConnectionMetrics;
import io.moquette.connections.MqttSession;
import io.moquette.connections.MqttSubscription;
import io.moquette.server.netty.metrics.BytesMetrics;
import io.moquette.server.netty.metrics.MessageMetrics;
import io.moquette.spi.ClientSession;
import io.moquette.spi.impl.SessionsRepository;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConnectionDescriptorStore implements IConnectionsManager {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionDescriptorStore.class);

    private final ConcurrentMap<String, ConnectionDescriptor> connectionDescriptors;
    private final SessionsRepository sessionsRepository;

    public ConnectionDescriptorStore(SessionsRepository sessionsRepository) {
        this.connectionDescriptors = new ConcurrentHashMap<>();
        this.sessionsRepository = sessionsRepository;
    }

    public boolean sendMessage(MqttMessage message, Integer messageID, String clientID) {
        final MqttMessageType messageType = message.fixedHeader().messageType();
        try {
            if (messageID != null) {
                LOG.info("Sending {} message CId=<{}>, messageId={}", messageType, clientID, messageID);
            } else {
                LOG.debug("Sending {} message CId=<{}>", messageType, clientID);
            }

            ConnectionDescriptor descriptor = connectionDescriptors.get(clientID);
            if (descriptor == null) {
                if (messageID != null) {
                    LOG.error("Client has just disconnected. {} message could not be sent. CId=<{}>, messageId={}",
                        messageType, clientID, messageID);
                } else {
                    LOG.error("Client has just disconnected. {} could not be sent. CId=<{}>", messageType, clientID);
                }
                /*
                 * If the client has just disconnected, its connection descriptor will be null. We
                 * don't have to make the broker crash: we'll just discard the PUBACK message.
                 */
                return false;
            }
            descriptor.writeAndFlush(message);
            return true;
        } catch (Throwable e) {
            String errorMsg = "Unable to send " + messageType + " message. CId=<" + clientID + ">";
            if (messageID != null) {
                errorMsg += ", messageId=" + messageID;
            }
            LOG.error(errorMsg, e);
            return false;
        }
    }

    public ConnectionDescriptor addConnection(ConnectionDescriptor descriptor) {
        return connectionDescriptors.putIfAbsent(descriptor.clientID, descriptor);
    }

    public boolean removeConnection(ConnectionDescriptor descriptor) {
        return connectionDescriptors.remove(descriptor.clientID, descriptor);
    }

    public ConnectionDescriptor getConnection(String clientID) {
        return connectionDescriptors.get(clientID);
    }

    @Override
    public boolean isConnected(String clientID) {
        return connectionDescriptors.containsKey(clientID);
    }

    @Override
    public int getActiveConnectionsNo() {
        return connectionDescriptors.size();
    }

    @Override
    public Collection<String> getConnectedClientIds() {
        return connectionDescriptors.keySet();
    }

    @Override
    public boolean closeConnection(String clientID, boolean closeImmediately) {
        ConnectionDescriptor descriptor = connectionDescriptors.get(clientID);
        if (descriptor == null) {
            LOG.error("Connection descriptor doesn't exist. MQTT connection cannot be closed. CId=<{}>, " +
                "closeImmediately={}", clientID, closeImmediately);
            return false;
        }
        if (closeImmediately) {
            descriptor.abort();
            return true;
        } else {
            return descriptor.close();
        }
    }

    @Override
    public Collection<MqttSession> getSessions() {
        LOG.info("Retrieving status of all sessions.");
        Collection<MqttSession> result = new ArrayList<>();
        for (ClientSession session : this.sessionsRepository.getAllSessions(true)) {
            result.add(buildMqttSession(session));
        }
        return result;
    }

    private MqttSession buildMqttSession(ClientSession session) {
        MqttSession result = new MqttSession();
        Collection<MqttSubscription> mqttSubscriptions = new ArrayList<>();
        for (Subscription subscription : session.getSubscriptions()) {
            mqttSubscriptions.add(new MqttSubscription(subscription.getRequestedQos().toString(),
                subscription.getClientId(), subscription.getTopicFilter().toString(), subscription.isActive()));
        }
        result.setActiveSubscriptions(mqttSubscriptions);
        result.setCleanSession(session.isCleanSession());
        ConnectionDescriptor descriptor = this.getConnection(session.clientID);
        result.setClientID(session.clientID);
        if (descriptor != null) {
            result.setConnectionEstablished(true);
            BytesMetrics bytesMetrics = descriptor.getBytesMetrics();
            MessageMetrics messageMetrics = descriptor.getMessageMetrics();
            result.setConnectionMetrics(new MqttConnectionMetrics(bytesMetrics.readBytes(), bytesMetrics.wroteBytes(),
                messageMetrics.messagesRead(), messageMetrics.messagesWrote()));
        } else {
            result.setConnectionEstablished(false);
        }
        result.setPendingPublishMessagesNo(session.getPendingPublishMessagesNo());
        result.setSecondPhaseAckPendingMessages(session.countPubReleaseWaitingPubComplete());
        result.setInflightMessages(session.getInflightMessagesNo());
        return result;
    }

}
