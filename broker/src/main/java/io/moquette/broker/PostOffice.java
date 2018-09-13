package io.moquette.broker;

import io.moquette.spi.impl.subscriptions.ISubscriptionsDirectory;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import io.moquette.spi.security.IAuthorizatorPolicy;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static io.moquette.spi.impl.Utils.messageId;
import static io.netty.channel.ChannelFutureListener.CLOSE_ON_FAILURE;
import static io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader.from;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import static io.netty.handler.codec.mqtt.MqttQoS.FAILURE;

class PostOffice {

    private static final Logger LOG = LoggerFactory.getLogger(PostOffice.class);

    private final ConcurrentMap<String, Queue> queues = new ConcurrentHashMap<>();
    private final Authorizator authorizator;
    private final ISubscriptionsDirectory subscriptions;

    PostOffice(ISubscriptionsDirectory subscriptions, IAuthorizatorPolicy authorizatorPolicy) {
        this.authorizator = new Authorizator(authorizatorPolicy);
        this.subscriptions = subscriptions;
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
                LOG.error("Topic filter is not valid. CId={}, topics: {}, offending topic filter: {}",
                          clientID, topics, topic);
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
}
