package io.moquette.broker;

import io.moquette.spi.impl.subscriptions.ISubscriptionsDirectory;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import io.moquette.spi.security.IAuthorizatorPolicy;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static io.moquette.spi.impl.Utils.messageId;
import static io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader.from;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import static io.netty.handler.codec.mqtt.MqttQoS.FAILURE;

class PostOffice {

    private static final class PublishedMessage {

        private final Topic topic;
        private final MqttQoS publishingQos;

        PublishedMessage(Topic topic, MqttQoS publishingQos) {
            this.topic = topic;
            this.publishingQos = publishingQos;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(PostOffice.class);

    private final ConcurrentMap<String, Queue> queues = new ConcurrentHashMap<>();
    private final Authorizator authorizator;
    private final ISubscriptionsDirectory subscriptions;
    private final IRetainedRepository retainedRepository;
    private SessionRegistry sessionRegistry;

    PostOffice(ISubscriptionsDirectory subscriptions, IAuthorizatorPolicy authorizatorPolicy,
               IRetainedRepository retainedRepository) {
        this.authorizator = new Authorizator(authorizatorPolicy);
        this.subscriptions = subscriptions;
        this.retainedRepository = retainedRepository;
    }

    public void init(SessionRegistry sessionRegistry) {
        this.sessionRegistry = sessionRegistry;
    }

    void dropQueuesForClient(String clientId) {
        queues.remove(clientId);
    }

    public void fireWill(Session.Will will) {
        // TODO
    }

    public void sendQueuedMessagesWhileOffline(String clientId) {
        // TODO
    }

    public void subscribeClientToTopics(MqttSubscribeMessage msg, String clientID, String username,
                                        MQTTConnection mqttConnection) {
        // verify which topics of the subscribe ongoing has read access permission
        int messageID = messageId(msg);
        List<MqttTopicSubscription> ackTopics = authorizator.verifyTopicsReadAccess(clientID, username, msg);
        MqttSubAckMessage ackMessage = doAckMessageFromValidateFilters(ackTopics, messageID);

        // store topics subscriptions in session
        List<Subscription> newSubscriptions = ackTopics.stream()
            .filter(req -> req.qualityOfService() != FAILURE)
            .map(req -> {
                final Topic topic = new Topic(req.topicName());
                return new Subscription(clientID, topic, req.qualityOfService());
            }).collect(Collectors.toList());

        for (Subscription subscription : newSubscriptions) {
            subscriptions.add(subscription);
        }

        // send ack message
        mqttConnection.sendSubAckMessage(messageID, ackMessage);

        //TODO  republish all retained messages matching the subscription topics
    }

    /**
     * Create the SUBACK response from a list of topicFilters
     */
    private MqttSubAckMessage doAckMessageFromValidateFilters(List<MqttTopicSubscription> topicFilters, int messageId) {
        List<Integer> grantedQoSLevels = new ArrayList<>();
        for (MqttTopicSubscription req : topicFilters) {
            grantedQoSLevels.add(req.qualityOfService().value());
        }

        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK, false, AT_MOST_ONCE,
                                                  false, 0);
        MqttSubAckPayload payload = new MqttSubAckPayload(grantedQoSLevels);
        return new MqttSubAckMessage(fixedHeader, from(messageId), payload);
    }

    public void unsubscribe(List<String> topics, MQTTConnection mqttConnection) {
        final String clientID = mqttConnection.getClientId();
        for (String t : topics) {
            Topic topic = new Topic(t);
            boolean validTopic = topic.isValid();
            if (!validTopic) {
                // close the connection, not valid topicFilter is a protocol violation
                mqttConnection.dropConnection();
                LOG.warn("Topic filter is not valid. CId={}, topics: {}, offending topic filter: {}", clientID,
                         topics, topic);
                return;
            }

            LOG.trace("Removing subscription. CId={}, topic={}", clientID, topic);
            subscriptions.removeSubscription(topic, clientID);

            // TODO remove the subscriptions to Session
//            clientSession.unsubscribeFrom(topic);

            //TODO notify interceptors
//            String username = NettyUtils.userName(channel);
//            m_interceptor.notifyTopicUnsubscribed(topic.toString(), clientID, username);
        }
    }

    void receivedPublishQos0(Topic topic, String username, String clientID, ByteBuf payload, boolean retain) {
        if (!authorizator.canWrite(topic, username, clientID)) {
            LOG.error("MQTT client is not authorized to publish on topic. CId={}, topic: {}", clientID, topic);
            return;
        }
        publish2Subscribers(payload, topic, AT_MOST_ONCE);

        if (retain) {
            // QoS == 0 && retain => clean old retained
            retainedRepository.cleanRetained(topic);
        }
// TODO
//        m_interceptor.notifyTopicPublished(msg, clientID, username);
    }

    private void publish2Subscribers(ByteBuf origPayload, Topic topic, MqttQoS publishingQos) {
        Set<Subscription> topicMatchingSubscriptions = subscriptions.matchWithoutQosSharpening(topic);

        for (final Subscription sub : topicMatchingSubscriptions) {
            MqttQoS qos = lowerQosToTheSubscriptionDesired(sub, publishingQos);
            Session targetSession = this.sessionRegistry.retrieve(sub.getClientId());

            boolean targetIsActive = targetSession != null && targetSession.connected();
            // TODO move all this logic into messageSender, which puts into the flightZone only the messages
            // that pull out of the queue.
            if (targetIsActive) {
                LOG.debug("Sending PUBLISH message to active subscriber CId: {}, topicFilter: {}, qos: {}",
                          sub.getClientId(), sub.getTopicFilter(), qos);
                // we need to retain because duplicate only copy r/w indexes and don't retain() causing
                // refCnt = 0
                ByteBuf payload = origPayload.retainedDuplicate();
                if (qos != MqttQoS.AT_MOST_ONCE) {
//                    // QoS 1 or 2
//                    int messageId = targetSession.inFlightAckWaiting(pubMsg);
//                    // set the PacketIdentifier only for QoS > 0
//                    publishMsg = notRetainedPublishWithMessageId(topic.toString(), qos, payload, messageId);
                } else {
                    targetSession.sendPublishNotRetained(topic, publishingQos, payload);
                }
//                this.messageSender.sendPublish(targetSession, publishMsg);
            } else {
                if (!targetSession.isClean()) {
                    LOG.debug("Storing pending PUBLISH inactive message. CId={}, topicFilter: {}, qos: {}",
                        sub.getClientId(), sub.getTopicFilter(), qos);
                    // store the message in targetSession queue to deliver
                    queues.get(sub.getClientId()).add(new PublishedMessage(topic, publishingQos/*, payload*/));
                }
            }
        }
    }

    static MqttQoS lowerQosToTheSubscriptionDesired(Subscription sub, MqttQoS qos) {
        if (qos.value() > sub.getRequestedQos().value()) {
            qos = sub.getRequestedQos();
        }
        return qos;
    }
}
