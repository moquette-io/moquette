package io.moquette.broker;

import io.moquette.server.netty.NettyUtils;
import io.moquette.spi.impl.DebugUtils;
import io.moquette.spi.impl.subscriptions.Topic;
import io.moquette.spi.security.IAuthenticator;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.netty.channel.ChannelFutureListener.CLOSE_ON_FAILURE;
import static io.netty.channel.ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.*;
import static io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader.from;
import static io.netty.handler.codec.mqtt.MqttQoS.*;

final class MQTTConnection {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTConnection.class);

    final Channel channel;
    private BrokerConfiguration brokerConfig;
    private IAuthenticator authenticator;
    private SessionRegistry sessionRegistry;
    private final PostOffice postOffice;
    private boolean connected;
    private final Map<Integer, MqttPublishMessage> qos2Receiving = new HashMap<>();
    private final Map<Integer, MqttPublishMessage> qos1Sending = new HashMap<>();
    private final Map<Integer, MqttPublishMessage> qos2SendingPhase1 = new HashMap<>();
    private final Set<Integer> qos2SendingPhase2 = new HashSet<>();

    MQTTConnection(Channel channel, BrokerConfiguration brokerConfig, IAuthenticator authenticator,
                   SessionRegistry sessionRegistry, PostOffice postOffice) {
        this.channel = channel;
        this.brokerConfig = brokerConfig;
        this.authenticator = authenticator;
        this.sessionRegistry = sessionRegistry;
        this.postOffice = postOffice;
        this.connected = false;
    }

    void handleMessage(MqttMessage msg) {
        MqttMessageType messageType = msg.fixedHeader().messageType();
        LOG.debug("Received MQTT message, type: {}, channel: {}", messageType, channel);
        switch (messageType) {
            case CONNECT:
                processConnect((MqttConnectMessage) msg);
                break;
            case SUBSCRIBE:
                processSubscribe((MqttSubscribeMessage) msg);
                break;
            case UNSUBSCRIBE:
                processUnsubscribe((MqttUnsubscribeMessage) msg);
                break;
            case PUBLISH:
                processPublish((MqttPublishMessage) msg);
                break;
            case PUBREC:
                processPubRec(msg);
                break;
            case PUBCOMP:
                processPubComp(msg);
                break;
            case PUBREL:
                processPubRel(msg);
                break;
            case DISCONNECT:
                processDisconnect(msg);
                break;
            case PUBACK:
                processPubAck(msg);
                break;
            case PINGREQ:
                MqttFixedHeader pingHeader = new MqttFixedHeader(MqttMessageType.PINGRESP,false, AT_MOST_ONCE,
                                                                false,0);
                MqttMessage pingResp = new MqttMessage(pingHeader);
                channel.writeAndFlush(pingResp).addListener(CLOSE_ON_FAILURE);
                break;
            default:
                LOG.error("Unknown MessageType: {}, channel: {}", messageType, channel);
                break;
        }
    }

    private void processPubComp(MqttMessage msg) {
        final int messageID = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
        qos2SendingPhase2.remove(messageID);
        // TODO notify the interceptor
//                final InterceptAcknowledgedMessage interceptAckMsg = new InterceptAcknowledgedMessage(inflightMsg, topic,
//                    username, messageID);
//                m_interceptor.notifyMessageAcknowledged(interceptAckMsg);
    }

    private void processPubRec(MqttMessage msg) {
        final int messageID = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
        qos2SendingPhase1.remove(messageID);
        qos2SendingPhase2.add(messageID);

        MqttFixedHeader pubRelHeader = new MqttFixedHeader(MqttMessageType.PUBREL, false, AT_LEAST_ONCE, false, 0);
        MqttMessage pubRelMessage = new MqttMessage(pubRelHeader, from(messageID));
        rawSend(pubRelMessage, messageID, getClientId());
    }

    private void processPubAck(MqttMessage msg) {
        // TODO remain to invoke in somehow m_interceptor.notifyMessageAcknowledged
        final int messageID = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
        qos1Sending.remove(messageID);
    }

    void processConnect(MqttConnectMessage msg) {
        MqttConnectPayload payload = msg.payload();
        String clientId = payload.clientIdentifier();
        final String username = payload.userName();
        LOG.trace("Processing CONNECT message. CId={} username: {} channel: {}", clientId, username, channel);

        if (isNotProtocolVersion(msg, MqttVersion.MQTT_3_1) && isNotProtocolVersion(msg, MqttVersion.MQTT_3_1_1)) {
            LOG.warn("MQTT protocol version is not valid. CId={} channel: {}", clientId, channel);
            abortConnection(CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION);
            return;
        }
        final boolean cleanSession = msg.variableHeader().isCleanSession();
        if (clientId == null || clientId.length() == 0) {
            if (!brokerConfig.isAllowZeroByteClientId()) {
                LOG.warn("Broker doesn't permit MQTT client ID empty. Username={}, channel: {}", username, channel);
                abortConnection(CONNECTION_REFUSED_IDENTIFIER_REJECTED);
                return;
            }

            if (!cleanSession) {
                LOG.warn("MQTT client ID cannot be empty for persistent session. Username={}, channel: {}", username, channel);
                abortConnection(CONNECTION_REFUSED_IDENTIFIER_REJECTED);
                return;
            }

            // Generating client id.
            clientId = UUID.randomUUID().toString().replace("-", "");
            LOG.debug("Client has connected with server generated id={}, username={}, channel: {}", clientId, username, channel);
        }

        if (!login(msg, clientId)) {
            abortConnection(CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
            channel.close().addListener(CLOSE_ON_FAILURE);
            return;
        }

        try {
            LOG.trace("Binding MQTTConnection (channel: {}) to session", channel);
            sessionRegistry.bindToSession(this, msg, clientId);
            NettyUtils.clientID(channel, clientId);
            LOG.trace("CONNACK sent, channel: {}", channel);
        } catch (SessionCorruptedException scex) {
            LOG.warn("MQTT session for client ID {} cannot be created, channel: {}", clientId, channel);
            abortConnection(CONNECTION_REFUSED_SERVER_UNAVAILABLE);
        }
    }

    private boolean isNotProtocolVersion(MqttConnectMessage msg, MqttVersion version) {
        return msg.variableHeader().version() != version.protocolLevel();
    }

    private void abortConnection(MqttConnectReturnCode returnCode) {
        MqttConnAckMessage badProto = connAck(returnCode, false);
        channel.writeAndFlush(badProto).addListener(FIRE_EXCEPTION_ON_FAILURE);
        channel.close().addListener(CLOSE_ON_FAILURE);
    }

    private MqttConnAckMessage connAck(MqttConnectReturnCode returnCode, boolean sessionPresent) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE,
            false, 0);
        MqttConnAckVariableHeader mqttConnAckVariableHeader = new MqttConnAckVariableHeader(returnCode, sessionPresent);
        return new MqttConnAckMessage(mqttFixedHeader, mqttConnAckVariableHeader);
    }

    private boolean login(MqttConnectMessage msg, final String clientId) {
        // handle user authentication
        if (msg.variableHeader().hasUserName()) {
            byte[] pwd = null;
            if (msg.variableHeader().hasPassword()) {
                pwd = msg.payload().password().getBytes(StandardCharsets.UTF_8);
            } else if (!brokerConfig.isAllowAnonymous()) {
                LOG.error("Client didn't supply any password and MQTT anonymous mode is disabled CId={}", clientId);
                return false;
            }
            final String login = msg.payload().userName();
            if (!authenticator.checkValid(clientId, login, pwd)) {
                LOG.error("Authenticator has rejected the MQTT credentials CId={}, username={}", clientId, login);
                return false;
            }
            NettyUtils.userName(channel, login);
        } else if (!brokerConfig.isAllowAnonymous()) {
            LOG.error("Client didn't supply any credentials and MQTT anonymous mode is disabled. CId={}", clientId);
            return false;
        }
        return true;
    }

    void handleConnectionLost() {
        String clientID = NettyUtils.clientID(channel);
        if (clientID != null && !clientID.isEmpty()) {
            LOG.info("Notifying connection lost event. CId = {}, channel: {}", clientID, channel);
            Session session = sessionRegistry.retrieve(clientID);
            if (session.hasWill()) {
                postOffice.fireWill(session.getWill());
            }
            sessionRegistry.remove(clientID);
        }
        channel.close().addListener(CLOSE_ON_FAILURE);
    }

    void sendConnAck(boolean isSessionAlreadyPresent) {
        connected = true;
        final MqttConnAckMessage ackMessage = connAck(CONNECTION_ACCEPTED, isSessionAlreadyPresent);
        channel.writeAndFlush(ackMessage);
    }

    void dropConnection() {
        channel.close();
    }

    void processDisconnect(MqttMessage msg) {
        final String clientID = NettyUtils.clientID(channel);
        LOG.trace("Start DISCONNECT CId={}, channel: {}", clientID, channel);
//        channel.flush();
        if (!connected) {
            LOG.info("DISCONNECT received on already closed connection, CId={}, channel: {}", clientID, channel);
            return;
        }
        sessionRegistry.disconnect(clientID);
        connected = false;
        channel.close();
        LOG.trace("Processed DISCONNECT CId={}, channel: {}", clientID, channel);
    }

    void processSubscribe(MqttSubscribeMessage msg) {
        final String clientID = NettyUtils.clientID(channel);
        if (!connected) {
            LOG.warn("SUBSCRIBE received on already closed connection, CId={}, channel: {}", clientID, channel);
            dropConnection();
            return;
        }
        postOffice.subscribeClientToTopics(msg, clientID, NettyUtils.userName(channel), this);
    }

    void sendSubAckMessage(int messageID, MqttSubAckMessage ackMessage) {
        final String clientId = NettyUtils.clientID(channel);
        LOG.trace("Sending SUBACK response CId={}, messageId: {}", clientId, messageID);
        channel.writeAndFlush(ackMessage).addListener(FIRE_EXCEPTION_ON_FAILURE);
    }

    private void processUnsubscribe(MqttUnsubscribeMessage msg) {
        List<String> topics = msg.payload().topics();
        String clientID = NettyUtils.clientID(channel);

        LOG.trace("Processing UNSUBSCRIBE message. CId={}, topics: {}", clientID, topics);
        postOffice.unsubscribe(topics, this, msg.variableHeader().messageId());
    }

    void sendUnsubAckMessage(List<String> topics, String clientID, int messageID) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, AT_MOST_ONCE,
            false, 0);
        MqttUnsubAckMessage ackMessage = new MqttUnsubAckMessage(fixedHeader, from(messageID));

        LOG.trace("Sending UNSUBACK message. CId={}, messageId: {}, topics: {}", clientID, messageID, topics);
        channel.writeAndFlush(ackMessage).addListener(FIRE_EXCEPTION_ON_FAILURE);
        LOG.trace("Client <{}> unsubscribed from topics <{}>", clientID, topics);
    }

    void processPublish(MqttPublishMessage msg) {
        final MqttQoS qos = msg.fixedHeader().qosLevel();
        final String username = NettyUtils.userName(channel);
        final String topicName = msg.variableHeader().topicName();
        final String clientId = getClientId();
        LOG.trace("Processing PUBLISH message. CId={}, topic: {}, messageId: {}, qos: {}", clientId, topicName,
                  msg.variableHeader().packetId(), qos);
        ByteBuf payload = msg.payload();
        final boolean retain = msg.fixedHeader().isRetain();
        final Topic topic = new Topic(topicName);
        if (!topic.isValid()) {
            LOG.debug("Drop connection because of invalid topic format");
            dropConnection();
        }
        switch (qos) {
            case AT_MOST_ONCE:
                postOffice.receivedPublishQos0(topic, username, clientId, payload, retain);
                break;
            case AT_LEAST_ONCE: {
                final int messageID = msg.variableHeader().packetId();
                postOffice.receivedPublishQos1(this, topic, username, payload, messageID, retain, msg);
                break;
            }
            case EXACTLY_ONCE: {
                final int messageID = msg.variableHeader().packetId();
                qos2Receiving.put(messageID, msg);
                msg.retain();
                sendPublishReceived(messageID);
                break;
            }
            default:
                LOG.error("Unknown QoS-Type:{}", qos);
                break;
        }
    }

    private void sendPublishReceived(int messageID) {
        LOG.trace("sendPubRec invoked on channel: {}", channel);
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBREC, false, AT_MOST_ONCE,
            false, 0);
        MqttPubAckMessage pubRecMessage = new MqttPubAckMessage(fixedHeader, from(messageID));
        rawSend(pubRecMessage, messageID, getClientId());
    }

    private void processPubRel(MqttMessage msg) {
        final int messageID = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
        final MqttPublishMessage mqttPublishMessage = qos2Receiving.get(messageID);
        postOffice.receivedPublishRelQos2(this, mqttPublishMessage, messageID);
        qos2Receiving.remove(messageID);
        mqttPublishMessage.release();
    }

    void sendPublish(MqttPublishMessage publishMsg) {
        final int messageId = publishMsg.variableHeader().packetId();
        final String topicName = publishMsg.variableHeader().topicName();
        final String clientId = getClientId();
        MqttQoS qos = publishMsg.fixedHeader().qosLevel();
        if (LOG.isTraceEnabled()) {
            LOG.trace("Sending PUBLISH({}) message. MessageId={}, CId={}, topic={}, payload={}", qos, messageId,
                      clientId, topicName, DebugUtils.payload2Str(publishMsg.payload()));
        } else {
            LOG.debug("Sending PUBLISH({}) message. MessageId={}, CId={}, topic={}", qos, messageId, clientId, topicName);
        }
        if (qos == AT_LEAST_ONCE) {
            // TODO generate an incremental packetID for outbound in flight messages
            qos1Sending.put(1, publishMsg);
        } else if (qos == EXACTLY_ONCE) {
            // TODO generate an incremental packetID for outbound in flight messages
            qos2SendingPhase1.put(1, publishMsg);
        }

        rawSend(publishMsg, messageId, clientId);
    }

    private void rawSend(MqttMessage msg, int messageId, String clientId) {
        boolean messageDelivered = false;
        try {
            channel.writeAndFlush(msg);
            messageDelivered = true;
        } catch (Throwable th) {
            LOG.error("Unable to send {} message. CId=<{}>, messageId={}", msg.fixedHeader().messageType(),
                clientId, messageId, th);
        }
        // TODO improve this for in-flight messages (qos != 0)
//
//        if (!messageDelivered) {
//            if (qos != AT_MOST_ONCE && !clientsession.isCleanSession()) {
//                LOG.warn("PUBLISH message could not be delivered. It will be stored. MessageId={}, CId={}, topic={}, " +
//                    "qos={}, removeTemporaryQoS2={}", messageId, clientId, topicName, qos, false);
//                clientsession.enqueue(asStoredMessage(pubMessage));
//            } else {
//                LOG.warn("PUBLISH message could not be delivered. It will be discarded. MessageId={}, CId={}, " +
//                    "topic={}, qos={}, removeTemporaryQoS2={}", messageId, clientId, topicName, qos, true);
//            }
//        }
    }

    void sendPubAck(int messageID) {
        LOG.trace("sendPubAck invoked");
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, false, AT_MOST_ONCE,
                                                  false, 0);
        MqttPubAckMessage pubAckMessage = new MqttPubAckMessage(fixedHeader, from(messageID));
        final String clientId = getClientId();
        rawSend(pubAckMessage, messageID, clientId);
    }

    void sendPubCompMessage(int messageID) {
        LOG.trace("Sending PUBCOMP message on channel: {}, messageId: {}", channel, messageID);
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBCOMP, false, AT_MOST_ONCE, false, 0);
        MqttMessage pubCompMessage = new MqttMessage(fixedHeader, from(messageID));
        final String clientId = getClientId();
        rawSend(pubCompMessage, messageID, clientId);
    }

    String getClientId() {
        return NettyUtils.clientID(channel);
    }

    @Override
    public String toString() {
        return "MQTTConnection{" +
            "channel=" + channel +
            ", connected=" + connected +
            '}';
    }
}
