package io.moquette.broker;

import io.moquette.server.netty.NettyUtils;
import io.moquette.spi.security.IAuthenticator;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static io.netty.channel.ChannelFutureListener.CLOSE_ON_FAILURE;
import static io.netty.channel.ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.*;
import static io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader.from;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;

final class MQTTConnection {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTConnection.class);

    private final Channel channel;
    private BrokerConfiguration brokerConfig;
    private IAuthenticator authenticator;
    private SessionRegistry sessionRegistry;
    private final PostOffice postOffice;
    private boolean connected;

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
//            case PUBLISH:
//                m_processor.processPublish(channel, (MqttPublishMessage) msg);
//                break;
//            case PUBREC:
//                m_processor.processPubRec(channel, msg);
//                break;
//            case PUBCOMP:
//                m_processor.processPubComp(channel, msg);
//                break;
//            case PUBREL:
//                m_processor.processPubRel(channel, msg);
//                break;
            case DISCONNECT:
                processDisconnect(msg);
                break;
//            case PUBACK:
//                m_processor.processPubAck(channel, (MqttPubAckMessage) msg);
//                break;
            case PINGREQ:
                MqttFixedHeader pingHeader = new MqttFixedHeader(
                        MqttMessageType.PINGRESP,
                        false,
                        AT_MOST_ONCE,
                        false,
                        0);
                MqttMessage pingResp = new MqttMessage(pingHeader);
                channel.writeAndFlush(pingResp).addListener(CLOSE_ON_FAILURE);
                break;
            default:
                LOG.error("Unknown MessageType: {}, channel: {}", messageType, channel);
                break;
        }
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
        } catch (/*SessionCorrupted*/Exception scex) {
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
            postOffice.fireWill(session.getWill());
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

    private void processSubscribe(MqttSubscribeMessage msg) {
        final String clientID = NettyUtils.clientID(channel);
        if (!connected) {
            LOG.warn("SUBSCRIBE received on already closed connection, CId={}, channel: {}", clientID, channel);
            dropConnection();
            return;
        }
        postOffice.subscribeClientToTopics(msg, clientID, NettyUtils.userName(channel), this);

        // TODO add the subscriptions to Session
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
        postOffice.unsubscribe(topics, this);

        // ack the client
        sendUnsubAckMessage(msg, topics, clientID);
    }

    private void sendUnsubAckMessage(MqttUnsubscribeMessage msg, List<String> topics, String clientID) {
        int messageID = msg.variableHeader().messageId();
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, AT_MOST_ONCE,
            false, 0);
        MqttUnsubAckMessage ackMessage = new MqttUnsubAckMessage(fixedHeader, from(messageID));

        LOG.trace("Sending UNSUBACK message. CId={}, messageId: {}, topics: {}", clientID, messageID, topics);
        channel.writeAndFlush(ackMessage).addListener(FIRE_EXCEPTION_ON_FAILURE);
        LOG.trace("Client <{}> unsubscribed from topics <{}>", clientID, topics);
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
