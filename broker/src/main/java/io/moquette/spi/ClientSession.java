/*
 * Copyright (c) 2012-2017 The original author or authorsgetRockQuestions()
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

package io.moquette.spi;

import io.moquette.spi.ISessionsStore.ClientTopicCouple;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

/**
 * Model a Session like describe on page 25 of MQTT 3.1.1 specification:
 * The Session state in the Server consists of:
 * <ul>
 *     <li>The existence of a Session, even if the rest of the Session state is empty.</li>
 *     <li>The Client’s subscriptions.</li>
 *     <li>QoS 1 and QoS 2 messages which have been sent to the Client, but have not been
 *     completely acknowledged.</li>
 *     <li>QoS 1 and QoS 2 messages pending transmission to the Client.</li>
 *     <li>QoS 2 messages which have been received from the Client, but have not been
 *     completely acknowledged.</li>
 *     <li>Optionally, QoS 0 messages pending transmission to the Client.</li>
 * </ul>
 *
 * @author andrea
 */
public class ClientSession {

    private final static Logger LOG = LoggerFactory.getLogger(ClientSession.class);

    public final String clientID;

    private final IMessagesStore messagesStore;

    private final ISessionsStore m_sessionsStore;

    private Set<Subscription> subscriptions = new HashSet<>();

    private volatile boolean cleanSession;

    // private BlockingQueue<AbstractMessage> m_queueToPublish = new
    // ArrayBlockingQueue<>(Constants.MAX_MESSAGE_QUEUE);

    public ClientSession(String clientID, IMessagesStore messagesStore, ISessionsStore sessionsStore,
            boolean cleanSession) {
        this.clientID = clientID;
        this.messagesStore = messagesStore;
        this.m_sessionsStore = sessionsStore;
        this.cleanSession = cleanSession;
    }

    /**
     * Return the list of persisted publishes for the given clientID. For QoS1 and QoS2 with clean
     * session flag, this method return the list of missed publish events while the client was
     * disconnected.
     *
     * @return the list of messages to be delivered for client related to the session.
     */
    public Queue<IMessagesStore.StoredMessage> queue() {
        LOG.info("Retrieving stored messages. MqttClientId = {}.", clientID);
        return this.m_sessionsStore.queue(clientID);
    }

    @Override
    public String toString() {
        return "ClientSession{clientID='" + clientID + '\'' + "}";
    }

    public boolean subscribe(Subscription newSubscription) {
        LOG.info(
                "Adding new subscription. MqttClientId = {}, topics = {}, qos = {}.",
                newSubscription.getClientId(),
                newSubscription.getTopicFilter(),
                newSubscription.getRequestedQos());
        boolean validTopic = newSubscription.getTopicFilter().isValid();
        if (!validTopic) {
            LOG.warn(
                    "The topic filter is not valid. MqttClientId = {}, topics = {}.",
                    newSubscription.getClientId(),
                    newSubscription.getTopicFilter());
            // send SUBACK with 0x80 for this topic filter
            return false;
        }
        ClientTopicCouple matchingCouple = new ClientTopicCouple(this.clientID, newSubscription.getTopicFilter());
        Subscription existingSub = m_sessionsStore.getSubscription(matchingCouple);
        // update the selected subscriptions if not present or if has a greater qos
        if (existingSub == null || existingSub.getRequestedQos().value() < newSubscription.getRequestedQos().value()) {
            if (existingSub != null) {
                LOG.info(
                        "The subscription already existed with a lower QoS value. It will be updated. "
                        + "MqttClientId = {}, topics = {}, existingQos = {}, newQos = {}.",
                        newSubscription.getClientId(),
                        newSubscription.getTopicFilter(),
                        existingSub.getRequestedQos(),
                        newSubscription.getRequestedQos());
                subscriptions.remove(newSubscription);
            }
            subscriptions.add(newSubscription);
            m_sessionsStore.addNewSubscription(newSubscription);
        }
        return true;
    }

    public void unsubscribeFrom(Topic topicFilter) {
        LOG.info("Removing subscription. MqttClientID = {}, topics = {}.", clientID, topicFilter);
        m_sessionsStore.removeSubscription(topicFilter, clientID);
        Set<Subscription> subscriptionsToRemove = new HashSet<>();
        for (Subscription sub : this.subscriptions) {
            if (sub.getTopicFilter().equals(topicFilter)) {
                subscriptionsToRemove.add(sub);
            }
        }
        subscriptions.removeAll(subscriptionsToRemove);
    }

    public void disconnect() {
        if (this.cleanSession) {
            LOG.info("The client has disconnected. Removing its subscriptions. MqttClientId = {}.", clientID);
            // cleanup topic subscriptions
            cleanSession();
        }
    }

    public void cleanSession() {
        LOG.info("Wiping existing subscriptions. MqttClientId = {}.", this.clientID);
        m_sessionsStore.wipeSubscriptions(this.clientID);

        // remove also the messages stored of type QoS1/2
        LOG.info("Removing stored messages with QoS 1 and 2. MqttClientId = {}.", this.clientID);
        messagesStore.dropMessagesInSession(this.clientID);
    }

    public boolean isCleanSession() {
        return this.cleanSession;
    }

    public void cleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
        this.m_sessionsStore.updateCleanStatus(this.clientID, cleanSession);
    }

    public int nextPacketId() {
        return this.m_sessionsStore.nextPacketID(this.clientID);
    }

    public void inFlightAcknowledged(int messageID) {
        if (LOG.isTraceEnabled())
            LOG.trace("Acknowledging inflight, clientID <{}> messageID {}", this.clientID, messageID);
        m_sessionsStore.inFlightAck(this.clientID, messageID);
    }

    public void inFlightAckWaiting(MessageGUID guid, int messageID) {
        if (LOG.isTraceEnabled())
            LOG.trace("Adding to inflight {}, guid <{}>", messageID, guid);
        m_sessionsStore.inFlight(this.clientID, messageID, guid);
    }

    public IMessagesStore.StoredMessage secondPhaseAcknowledged(int messageID) {
        MessageGUID guid = m_sessionsStore.secondPhaseAcknowledged(clientID, messageID);
        return messagesStore.getMessageByGuid(guid);
    }

    /**
     * Enqueue a message to be sent to the client.
     *
     * @param message
     *            the message to enqueue.
     */
    public void enqueue(IMessagesStore.StoredMessage message) {
        this.m_sessionsStore.queue(this.clientID).add(message);
    }

    public IMessagesStore.StoredMessage storedMessage(int messageID) {
        final MessageGUID guid = m_sessionsStore.mapToGuid(clientID, messageID);
        return messagesStore.getMessageByGuid(guid);
    }

    public void moveInFlightToSecondPhaseAckWaiting(int messageID) {
        m_sessionsStore.moveInFlightToSecondPhaseAckWaiting(this.clientID, messageID);
    }

    public IMessagesStore.StoredMessage getInflightMessage(int messageID) {
        return m_sessionsStore.getInflightMessage(clientID, messageID);
    }

    public Set<Subscription> getSubscriptions() {
        return subscriptions;
    }

    public int getPendingPublishMessagesNo() {
        return m_sessionsStore.getPendingPublishMessagesNo(clientID);
    }

    public int getSecondPhaseAckPendingMessages() {
        return m_sessionsStore.getSecondPhaseAckPendingMessages(clientID);
    }

    public int getInflightMessagesNo() {
        return m_sessionsStore.getInflightMessagesNo(clientID);
    }

}
