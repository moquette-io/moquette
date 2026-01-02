/*
 * Copyright (c) 2012-2018 The original author or authors
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
package io.moquette.broker;

import io.moquette.broker.subscriptions.Topic;
import io.moquette.broker.security.IAuthorizatorPolicy;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static io.moquette.broker.Utils.messageId;
import static io.netty.handler.codec.mqtt.MqttQoS.FAILURE;

final class Authorizator {

    private static final Logger LOG = LoggerFactory.getLogger(Authorizator.class);

    private final IAuthorizatorPolicy policy;

    // Contains the list of topic-client that has read access forced on reply topic.
    private ConcurrentMap<Utils.Couple<Topic, String>, Boolean> responseTopicForcedReads = new ConcurrentHashMap<>();
    // Contains the list of requesters' reply topics that need write access by all the other (responders).
    private ConcurrentMap<Topic, Boolean> responseTopicForcedWrites = new ConcurrentHashMap<>();

    Authorizator(IAuthorizatorPolicy policy) {
        this.policy = policy;
    }


    List<MqttTopicSubscription> verifyAlsoSharedTopicsReadAccess(String clientID, String username, MqttSubscribeMessage msg) {
        return verifyTopicsReadAccessWithTopicExtractor(clientID, username, msg, Authorizator::extractShareTopic);
    }

    private static Topic extractShareTopic(String s) {
        if (SharedSubscriptionUtils.isSharedSubscription(s)) {
            return Topic.asTopic(SharedSubscriptionUtils.extractFilterFromShared(s));
        }
        return Topic.asTopic(s);
    }

    /**
     * @param clientID
     *            the clientID
     * @param username
     *            the username
     * @param msg
     *            the subscribe message to verify
     * @return the list of verified topics for the given subscribe message.
     */
    List<MqttTopicSubscription> verifyTopicsReadAccess(String clientID, String username, MqttSubscribeMessage msg) {
        return verifyTopicsReadAccessWithTopicExtractor(clientID, username, msg, Topic::asTopic);
    }

    private List<MqttTopicSubscription> verifyTopicsReadAccessWithTopicExtractor(String clientID, String username,
                                                 MqttSubscribeMessage msg, Function<String, Topic> topicExtractor) {
        List<MqttTopicSubscription> ackTopics = new ArrayList<>();

        final int messageId = messageId(msg);
        for (MqttTopicSubscription req : msg.payload().topicSubscriptions()) {
            Topic topic = topicExtractor.apply(req.topicName());
            final MqttQoS qos = getQoSCheckingAlsoPermissionsOnTopic(clientID, username, messageId, topic,
                req.qualityOfService());
            MqttSubscriptionOption option = PostOffice.optionWithQos(qos, req.option());
            ackTopics.add(new MqttTopicSubscription(req.topicName(), option));
        }
        return ackTopics;
    }

    private MqttQoS getQoSCheckingAlsoPermissionsOnTopic(String clientID, String username, int messageId,
                                                         Topic topic, MqttQoS requestedQoS) {
        if (policy.canRead(topic, username, clientID)) {
            if (topic.isValid()) {
                LOG.debug("Client will be subscribed to the topic username: {}, messageId: {}, topic: {}",
                    username, messageId, topic);
                return requestedQoS;
            }

            LOG.warn("Topic filter is not valid username: {}, messageId: {}, topic: {}",
                username, messageId, topic);
            return FAILURE;
        }

        // send SUBACK with 0x80, the user hasn't credentials to read the topic
        LOG.warn("Client does not have read permissions on the topic username: {}, messageId: {}, " +
                 "topic: {}", username, messageId, topic);
        return FAILURE;
    }

    /**
     * Ask the authorization policy if the topic can be used in a publish.
     *
     * @param topic
     *            the topic to write to.
     * @param user
     *            the user
     * @param client
     *            the client
     * @return true if the user from client can publish data on topic.
     */
    boolean canWrite(Topic topic, String user, String client) {
        boolean policyResult = policy.canWrite(topic, user, client);
        if (!policyResult && responseTopicForcedWrites.containsKey(topic)) {
            LOG.warn("Found write discord by policy and response information topic configured. The policy prohibit " +
                "while the response topic should be accessible for all to write. topic: {}", topic);
            return true;
        }
        return policyResult;
    }

    boolean canRead(Topic topic, String user, String client) {
        boolean policyResult = policy.canRead(topic, user, client);
        if (!policyResult && responseTopicForcedReads.containsKey(Utils.Couple.of(topic, client))) {
            LOG.warn("Found read discord by policy and response information topic configured. The policy prohibit " +
                "while the response topic should be accessible by read from client{}. topic: {}", client, topic);
            return true;
        }
        return policyResult;
    }

    void forceReadAccess(Topic topic, String client) {
        responseTopicForcedReads.putIfAbsent(Utils.Couple.of(topic, client), true);
    }

    public void forceWriteToAll(Topic topic) {
        responseTopicForcedWrites.putIfAbsent(topic, true);
    }
}
