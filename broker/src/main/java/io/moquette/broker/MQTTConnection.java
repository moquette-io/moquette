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

import io.moquette.BrokerConstants;
import io.moquette.broker.security.IAuthenticator;
import io.moquette.broker.security.PemUtils;
import io.moquette.broker.subscriptions.Topic;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.codec.mqtt.MqttMessageBuilders.ConnAckPropertiesBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLPeerUnverifiedException;

import static io.moquette.BrokerConstants.INFLIGHT_WINDOW_SIZE;
import static io.netty.channel.ChannelFutureListener.CLOSE_ON_FAILURE;
import static io.netty.channel.ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.*;
import static io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader.from;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_LEAST_ONCE;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;

final class MQTTConnection {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTConnection.class);

    static final boolean sessionLoopDebug = Boolean.parseBoolean(System.getProperty("moquette.session_loop.debug", "false"));
    private static final int UNDEFINED_VERSION = -1;

    final Channel channel;
    private final BrokerConfiguration brokerConfig;
    private final IAuthenticator authenticator;
    private final SessionRegistry sessionRegistry;
    private final PostOffice postOffice;
    private volatile boolean connected;
    private final AtomicInteger lastPacketId = new AtomicInteger(0);
    private Session bindedSession;
    private int protocolVersion;

    MQTTConnection(Channel channel, BrokerConfiguration brokerConfig, IAuthenticator authenticator,
                   SessionRegistry sessionRegistry, PostOffice postOffice) {
        this.channel = channel;
        this.brokerConfig = brokerConfig;
        this.authenticator = authenticator;
        this.sessionRegistry = sessionRegistry;
        this.postOffice = postOffice;
        this.connected = false;
        this.protocolVersion = UNDEFINED_VERSION;
    }

    void handleMessage(MqttMessage msg) {
        MqttMessageType messageType = msg.fixedHeader().messageType();
        LOG.debug("Received MQTT message, type: {}", messageType);
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
                MqttFixedHeader pingHeader = new MqttFixedHeader(MqttMessageType.PINGRESP, false, AT_MOST_ONCE,
                                                                false, 0);
                MqttMessage pingResp = new MqttMessage(pingHeader);
                channel.writeAndFlush(pingResp).addListener(CLOSE_ON_FAILURE);
                break;
            default:
                LOG.error("Unknown MessageType: {}", messageType);
                break;
        }
    }

    private void processPubComp(MqttMessage msg) {
        final int messageID = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
        final String clientID = bindedSession.getClientID();
        this.postOffice.routeCommand(clientID, "PUBCOMP", () -> {
            checkMatchSessionLoop(clientID);
            bindedSession.processPubComp(messageID);
            return clientID;
        });
    }

    private void processPubRec(MqttMessage msg) {
        final int messageID = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
        final String clientID = bindedSession.getClientID();
        this.postOffice.routeCommand(clientID, "PUBREC", () -> {
            checkMatchSessionLoop(clientID);
            bindedSession.processPubRec(messageID);
            return null;
        });
    }

    static MqttMessage pubrel(int messageID) {
        MqttFixedHeader pubRelHeader = new MqttFixedHeader(MqttMessageType.PUBREL, false, AT_LEAST_ONCE, false, 0);
        return new MqttMessage(pubRelHeader, from(messageID));
    }

    private void processPubAck(MqttMessage msg) {
        final int messageID = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
        final String clientId = getClientId();
        this.postOffice.routeCommand(clientId, "PUB ACK", () -> {
            checkMatchSessionLoop(clientId);
            bindedSession.pubAckReceived(messageID);
            return null;
        });
    }

    PostOffice.RouteResult processConnect(MqttConnectMessage msg) {
        MqttConnectPayload payload = msg.payload();
        String clientId = payload.clientIdentifier();
        final String username = payload.userName();
        LOG.trace("Processing CONNECT message. CId: {} username: {}", clientId, username);

        if (isNotProtocolVersion(msg, MqttVersion.MQTT_3_1) &&
            isNotProtocolVersion(msg, MqttVersion.MQTT_3_1_1) &&
            isNotProtocolVersion(msg, MqttVersion.MQTT_5)
        ) {
            LOG.warn("MQTT protocol version is not valid. CId: {}", clientId);
            abortConnection(CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION);
            return PostOffice.RouteResult.failed(clientId);
        }
        final boolean cleanSession = msg.variableHeader().isCleanSession();
        final boolean serverGeneratedClientId;
        if (clientId == null || clientId.length() == 0) {
            if (isNotProtocolVersion(msg, MqttVersion.MQTT_5)) {
                if (!brokerConfig.isAllowZeroByteClientId()) {
                    LOG.info("Broker doesn't permit MQTT empty client ID. Username: {}", username);
                    abortConnection(CONNECTION_REFUSED_IDENTIFIER_REJECTED);
                    return PostOffice.RouteResult.failed(clientId);
                }

                if (!cleanSession) {
                    LOG.info("MQTT client ID cannot be empty for persistent session. Username: {}", username);
                    abortConnection(CONNECTION_REFUSED_IDENTIFIER_REJECTED);
                    return PostOffice.RouteResult.failed(clientId);
                }
            }

            // Generating client id.
            clientId = UUID.randomUUID().toString().replace("-", "");
            serverGeneratedClientId = true;
            LOG.debug("Client has connected with integration generated id: {}, username: {}", clientId, username);
        } else {
            serverGeneratedClientId = false;
        }

        if (!login(msg, clientId)) {
            if (isProtocolVersion(msg, MqttVersion.MQTT_5)) {
                final ConnAckPropertiesBuilder builder = prepareConnAckPropertiesBuilder(false, clientId);
                builder.reasonString("User credentials provided are not recognized as valid");
                abortConnectionV5(CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD, builder);
            } else {
                abortConnection(CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
            }

            channel.close().addListener(CLOSE_ON_FAILURE);
            return PostOffice.RouteResult.failed(clientId);
        }

        final String sessionId = clientId;
        protocolVersion = msg.variableHeader().version();
        return postOffice.routeCommand(clientId, "CONN", () -> {
            checkMatchSessionLoop(sessionId);
            executeConnect(msg, sessionId, serverGeneratedClientId);
            return null;
        });
    }

    private void checkMatchSessionLoop(String clientId) {
        if (!sessionLoopDebug) {
            return;
        }
        final String currentThreadName = Thread.currentThread().getName();
        final String expectedThreadName = postOffice.sessionLoopThreadName(clientId);
        if (!expectedThreadName.equals(currentThreadName)) {
            throw new IllegalStateException("Expected to be executed on thread " + expectedThreadName + " but running on " + currentThreadName + ". This means a programming error");
        }
    }

    /**
     * Invoked by the Session's event loop.
     * */
    private void executeConnect(MqttConnectMessage msg, String clientId, boolean serverGeneratedClientId) {
        if (isProtocolVersion(msg, MqttVersion.MQTT_5)) {
            // if MQTT5 validate the payload of the will
            boolean hasPayloadFormatIndicator = extractWillPayloadFormatIndicator(msg.payload().willProperties());
            if (hasPayloadFormatIndicator) {
                byte[] willPayload = msg.payload().willMessageInBytes();
                boolean validWillPayload = checkUTF8Validity(willPayload);
                if (!validWillPayload) {
                    final ConnAckPropertiesBuilder builder = prepareConnAckPropertiesBuilder(false, clientId);
                    builder.reasonString("Not UTF8 payload in Will");
                    abortConnectionV5(CONNECTION_REFUSED_PAYLOAD_FORMAT_INVALID, builder);
                    return;
                }
            }
        }

        final SessionRegistry.SessionCreationResult result;
        try {
            LOG.trace("Binding MQTTConnection to session");
            result = sessionRegistry.createOrReopenSession(msg, clientId, this.getUsername());
            result.session.bind(this);
            bindedSession = result.session;
        } catch (SessionCorruptedException scex) {
            LOG.warn("MQTT session for client ID {} cannot be created", clientId);
            if (isProtocolVersion(msg, MqttVersion.MQTT_5)) {
                final ConnAckPropertiesBuilder builder = prepareConnAckPropertiesBuilder(false, clientId);
                builder.reasonString("Error creating the session, retry later");
                abortConnectionV5(CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID, builder);
            } else {
                abortConnection(CONNECTION_REFUSED_SERVER_UNAVAILABLE);
            }
            return;
        }
        NettyUtils.clientID(channel, clientId);

        final boolean msgCleanSessionFlag = msg.variableHeader().isCleanSession();
        // [MQTT-3.2.2-2, MQTT-3.2.2-3, MQTT-3.2.2-6]
        boolean isSessionAlreadyPresent = !msgCleanSessionFlag && result.alreadyStored;
        final String clientIdUsed = clientId;
        final MqttMessageBuilders.ConnAckBuilder connAckBuilder = MqttMessageBuilders.connAck()
            .returnCode(CONNECTION_ACCEPTED)
            .sessionPresent(isSessionAlreadyPresent);
        if (isProtocolVersion(msg, MqttVersion.MQTT_5)) {
            // set properties for MQTT 5
            final MqttProperties ackProperties = prepareConnAckProperties(serverGeneratedClientId, clientId);
            connAckBuilder.properties(ackProperties);
        }
        final MqttConnAckMessage ackMessage = connAckBuilder.build();
        channel.writeAndFlush(ackMessage).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    LOG.trace("CONNACK sent, channel: {}", channel);
                    if (!result.session.completeConnection()) {
                        // send DISCONNECT and close the channel
                        final MqttMessage disconnectMsg = MqttMessageBuilders.disconnect().build();
                        channel.writeAndFlush(disconnectMsg).addListener(CLOSE);
                        LOG.warn("CONNACK is sent but the session created can't transition in CONNECTED state");
                    } else {
                        connected = true;
                        // OK continue with sending queued messages and normal flow

                        postOffice.wipeExistingScheduledWill(clientIdUsed);

                        if (result.mode == SessionRegistry.CreationModeEnum.REOPEN_EXISTING) {
                            final Session session = result.session;
                            postOffice.routeCommand(session.getClientID(), "sendOfflineMessages", () -> {
                                session.reconnectSession();
                                return null;
                            });
                        }

                        initializeKeepAliveTimeout(channel, msg, clientIdUsed);
                        if (isNotProtocolVersion(msg, MqttVersion.MQTT_5)) {
                            // In MQTT5 MQTT-4.4.0-1 avoid retries messages on timer base.
                            setupInflightResender(channel);
                        }

                        postOffice.dispatchConnection(msg);
                        LOG.trace("dispatch connection: {}", msg);
                    }
                } else {
                    sessionRegistry.connectionClosed(bindedSession);
                    LOG.error("CONNACK send failed, cleanup session and close the connection", future.cause());
                    channel.close();
                }

            }
        });
    }

    /**
     * @return the value of the Payload Format Indicator property from Will specification.
     * */
    private static boolean extractWillPayloadFormatIndicator(MqttProperties mqttProperties) {
        final MqttProperties.MqttProperty<Integer> payloadFormatIndicatorProperty =
            mqttProperties.getProperty(MqttProperties.MqttPropertyType.PAYLOAD_FORMAT_INDICATOR.value());
        boolean hasPayloadFormatIndicator = false;
        if (payloadFormatIndicatorProperty != null) {
            int payloadFormatIndicator = payloadFormatIndicatorProperty.value();
            hasPayloadFormatIndicator = payloadFormatIndicator == 1;
        }
        return hasPayloadFormatIndicator;
    }

    private static boolean checkUTF8Validity(byte[] rawBytes) {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
        try {
            decoder.decode(ByteBuffer.wrap(rawBytes));
        } catch (CharacterCodingException ex) {
            return false;
        }
        return true;
    }

    private MqttProperties prepareConnAckProperties(boolean serverGeneratedClientId, String clientId) {
        return prepareConnAckPropertiesBuilder(serverGeneratedClientId, clientId).build();
    }

    private ConnAckPropertiesBuilder prepareConnAckPropertiesBuilder(boolean serverGeneratedClientId, String clientId) {
        final ConnAckPropertiesBuilder builder = new ConnAckPropertiesBuilder();
        // default maximumQos is 2, [MQTT-3.2.2-10]
        // unlimited maximumPacketSize inside however the protocol limit
        if (serverGeneratedClientId) {
            builder.assignedClientId(clientId);
        }

        builder
            .sessionExpiryInterval(BrokerConstants.INFINITE_SESSION_EXPIRY)
            .receiveMaximum(INFLIGHT_WINDOW_SIZE)
            .retainAvailable(true)
            .wildcardSubscriptionAvailable(true)
            .subscriptionIdentifiersAvailable(false)
            .sharedSubscriptionAvailable(false);
        return builder;
    }

    private void setupInflightResender(Channel channel) {
        channel.pipeline()
            .addFirst("inflightResender", new InflightResender(5_000, TimeUnit.MILLISECONDS));
    }

    private void initializeKeepAliveTimeout(Channel channel, MqttConnectMessage msg, String clientId) {
        int keepAlive = msg.variableHeader().keepAliveTimeSeconds();
        NettyUtils.keepAlive(channel, keepAlive);
        NettyUtils.cleanSession(channel, msg.variableHeader().isCleanSession());
        NettyUtils.clientID(channel, clientId);
        int idleTime = Math.round(keepAlive * 1.5f);
        setIdleTime(channel.pipeline(), idleTime);

        LOG.debug("Connection has been configured CId={}, keepAlive={}, removeTemporaryQoS2={}, idleTime={}",
            clientId, keepAlive, msg.variableHeader().isCleanSession(), idleTime);
    }

    private void setIdleTime(ChannelPipeline pipeline, int idleTime) {
        if (pipeline.names().contains("idleStateHandler")) {
            pipeline.remove("idleStateHandler");
        }
        pipeline.addFirst("idleStateHandler", new IdleStateHandler(idleTime, 0, 0));
    }

    private static boolean isNotProtocolVersion(MqttConnectMessage msg, MqttVersion version) {
        return !isProtocolVersion(msg, version);
    }

    private static boolean isProtocolVersion(MqttConnectMessage msg, MqttVersion version) {
        return msg.variableHeader().version() == version.protocolLevel();
    }

    private void abortConnection(MqttConnectReturnCode returnCode) {
        MqttConnAckMessage badProto = MqttMessageBuilders.connAck()
            .returnCode(returnCode)
            .sessionPresent(false).build();
        channel.writeAndFlush(badProto).addListener(FIRE_EXCEPTION_ON_FAILURE);
        channel.close().addListener(CLOSE_ON_FAILURE);
    }

    private void abortConnectionV5(MqttConnectReturnCode returnCode, ConnAckPropertiesBuilder propertiesBuilder) {
        MqttConnAckMessage badProto = MqttMessageBuilders.connAck()
            .returnCode(returnCode)
            .properties(propertiesBuilder.build())
            .sessionPresent(false).build();
        channel.writeAndFlush(badProto).addListener(FIRE_EXCEPTION_ON_FAILURE);
        channel.close().addListener(CLOSE_ON_FAILURE);
    }

    private boolean login(MqttConnectMessage msg, final String clientId) {
        String userName = null;
        byte[] pwd = null;

        if (msg.variableHeader().hasUserName()) {
            userName = msg.payload().userName();
            // MQTT 3.1.2.9 does not mandate that there is a password - let the authenticator determine if it's needed
            if (msg.variableHeader().hasPassword()) {
                pwd = msg.payload().passwordInBytes();
            }
        }

        if (brokerConfig.isPeerCertificateAsUsername()) {
            // Use peer cert as username
            userName = readClientProvidedCertificates(clientId);
        }

        if (userName == null || userName.isEmpty()) {
            if (brokerConfig.isAllowAnonymous()) {
                return true;
            } else {
                LOG.info("Client didn't supply any credentials and MQTT anonymous mode is disabled. CId={}", clientId);
                return false;
            }
        }

        if (!authenticator.checkValid(clientId, userName, pwd)) {
            LOG.info("Authenticator has rejected the MQTT credentials CId={}, username={}", clientId, userName);
            return false;
        }
        NettyUtils.userName(channel, userName);
        return true;
    }

    private String readClientProvidedCertificates(String clientId) {
        try {
            SslHandler sslhandler = (SslHandler) channel.pipeline().get("ssl");
            if (sslhandler != null) {
                Certificate[] certificateChain = sslhandler.engine().getSession().getPeerCertificates();
                return PemUtils.certificatesToPem(certificateChain);
            }
        } catch (SSLPeerUnverifiedException e) {
            LOG.debug("No peer cert provided. CId={}", clientId);
        } catch (CertificateEncodingException | IOException e) {
            LOG.warn("Unable to decode client certificate. CId={}", clientId);
        }
        return null;
    }

    void handleConnectionLost() {
        final String clientID = NettyUtils.clientID(channel);
        if (clientID == null || clientID.isEmpty()) {
            return;
        }
        // this must not be done on the netty thread
        LOG.debug("Notifying connection lost event");
        postOffice.routeCommand(clientID, "CONN LOST", () -> {
            checkMatchSessionLoop(clientID);
            if (isBoundToSession() || isSessionUnbound()) {
                LOG.debug("Cleaning {}", clientID);
                processConnectionLost(clientID);
            } else {
                LOG.debug("NOT Cleaning {}, bound to other connection.", clientID);
            }
            return null;
        });
    }

    // Invoked when a TCP connection drops and not when a client send DISCONNECT and close.
    private void processConnectionLost(String clientID) {
        if (bindedSession.hasWill()) {
            postOffice.fireWill(bindedSession);
        }
        if (bindedSession.connected()) {
            LOG.debug("Closing session on connectionLost {}", clientID);
            sessionRegistry.connectionClosed(bindedSession);
            connected = false;
        }
        //dispatch connection lost to intercept.
        String userName = NettyUtils.userName(channel);
        postOffice.dispatchConnectionLost(clientID,userName);
        LOG.trace("dispatch disconnection: userName={}", userName);
    }

    boolean isConnected() {
        return connected;
    }

    void dropConnection() {
        channel.close().addListener(FIRE_EXCEPTION_ON_FAILURE);
    }

    PostOffice.RouteResult processDisconnect(MqttMessage msg) {
        final String clientID = NettyUtils.clientID(channel);
        LOG.trace("Start DISCONNECT");
        if (!connected) {
            LOG.info("DISCONNECT received on already closed connection");
            return PostOffice.RouteResult.success(clientID, CompletableFuture.completedFuture(null));
        }

        return this.postOffice.routeCommand(clientID, "DISCONN", () -> {
            checkMatchSessionLoop(clientID);
            if (!isBoundToSession()) {
                LOG.debug("NOT processing disconnect {}, not bound.", clientID);
                return null;
            }
            if (isProtocolVersion5()) {
                MqttReasonCodeAndPropertiesVariableHeader disconnectHeader = (MqttReasonCodeAndPropertiesVariableHeader) msg.variableHeader();
                if (disconnectHeader.reasonCode() != MqttReasonCodes.Disconnect.NORMAL_DISCONNECT.byteValue()) {
                    // handle the will
                    if (bindedSession.hasWill()) {
                        postOffice.fireWill(bindedSession);
                    }
                }
            }
            LOG.debug("Closing session on disconnect {}", clientID);
            sessionRegistry.connectionClosed(bindedSession);
            connected = false;
            protocolVersion = UNDEFINED_VERSION;
            channel.close().addListener(FIRE_EXCEPTION_ON_FAILURE);
            String userName = NettyUtils.userName(channel);
            postOffice.clientDisconnected(clientID, userName);
            LOG.trace("dispatch disconnection userName={}", userName);
            return null;
        });
    }

    boolean isProtocolVersion5() {
        return protocolVersion == MqttVersion.MQTT_5.protocolLevel();
    }

    PostOffice.RouteResult processSubscribe(MqttSubscribeMessage msg) {
        final String clientID = NettyUtils.clientID(channel);
        if (!connected) {
            LOG.warn("SUBSCRIBE received on already closed connection");
            dropConnection();
            return PostOffice.RouteResult.success(clientID, CompletableFuture.completedFuture(null));
        }
        final String username = NettyUtils.userName(channel);
        return postOffice.routeCommand(clientID, "SUB", () -> {
            checkMatchSessionLoop(clientID);
            if (isBoundToSession())
                postOffice.subscribeClientToTopics(msg, clientID, username, this);
            return null;
        });
    }

    void sendSubAckMessage(int messageID, MqttSubAckMessage ackMessage) {
        LOG.trace("Sending SUBACK response messageId: {}", messageID);
        channel.writeAndFlush(ackMessage).addListener(FIRE_EXCEPTION_ON_FAILURE);
    }

    private void processUnsubscribe(MqttUnsubscribeMessage msg) {
        List<String> topics = msg.payload().topics();
        final String clientID = NettyUtils.clientID(channel);
        final int messageId = msg.variableHeader().messageId();

        postOffice.routeCommand(clientID, "UNSUB", () -> {
            checkMatchSessionLoop(clientID);
            if (!isBoundToSession())
                return null;
            LOG.trace("Processing UNSUBSCRIBE message. topics: {}", topics);
            postOffice.unsubscribe(topics, this, messageId);
            return null;
        });
    }

    void sendUnsubAckMessage(List<String> topics, String clientID, int messageID) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, AT_MOST_ONCE,
            false, 0);
        MqttUnsubAckMessage ackMessage = new MqttUnsubAckMessage(fixedHeader, from(messageID));

        LOG.trace("Sending UNSUBACK message. messageId: {}, topics: {}", messageID, topics);
        channel.writeAndFlush(ackMessage).addListener(FIRE_EXCEPTION_ON_FAILURE);
        LOG.trace("Client unsubscribed from topics <{}>", topics);
    }

    PostOffice.RouteResult processPublish(MqttPublishMessage msg) {
        final MqttQoS qos = msg.fixedHeader().qosLevel();
        final String username = NettyUtils.userName(channel);
        final String topicName = msg.variableHeader().topicName();
        final String clientId = getClientId();
        final int messageID = msg.variableHeader().packetId();
        LOG.trace("Processing PUBLISH message, topic: {}, messageId: {}, qos: {}", topicName, messageID, qos);
        final Topic topic = new Topic(topicName);
        if (!topic.isValid()) {
            LOG.debug("Drop connection because of invalid topic format");
            dropConnection();
        }

        if (!topic.isEmpty() && topic.headToken().isReserved()) {
            LOG.warn("Avoid to publish on topic which contains reserved topic (starts with $)");
            return PostOffice.RouteResult.failed(clientId);
        }

        // retain else msg is cleaned by the NewNettyMQTTHandler and is not available
        // in execution by SessionEventLoop
        msg.retain();
        switch (qos) {
            case AT_MOST_ONCE:
                return postOffice.routeCommand(clientId, "PUB QoS0", () -> {
                    checkMatchSessionLoop(clientId);
                    if (!isBoundToSession()) {
                        return null;
                    }
                    postOffice.receivedPublishQos0(topic, username, clientId, msg);
                    return null;
                }).ifFailed(msg::release);
            case AT_LEAST_ONCE:
                return postOffice.routeCommand(clientId, "PUB QoS1", () -> {
                    checkMatchSessionLoop(clientId);
                    if (!isBoundToSession())
                        return null;
                    postOffice.receivedPublishQos1(this, topic, username, messageID, msg);
                    return null;
                }).ifFailed(msg::release);
            case EXACTLY_ONCE: {
                final PostOffice.RouteResult firstStepResult = postOffice.routeCommand(clientId, "PUB QoS2", () -> {
                    checkMatchSessionLoop(clientId);
                    if (!isBoundToSession())
                        return null;
                    bindedSession.receivedPublishQos2(messageID, msg);
                    return null;
                });
                if (!firstStepResult.isSuccess()) {
                    msg.release();
                    LOG.trace("Failed to enqueue PUB QoS2 to session loop for  {}", clientId);
                    return firstStepResult;
                }
                firstStepResult.completableFuture().thenRun(() ->
                    postOffice.receivedPublishQos2(this, msg, username).completableFuture()
                );
                return firstStepResult;
            }
            default:
                LOG.error("Unknown QoS-Type:{}", qos);
                return PostOffice.RouteResult.failed(clientId, "Unknown QoS-");
        }
    }

    void sendPubRec(int messageID) {
        LOG.trace("sendPubRec invoked, messageID: {}", messageID);
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBREC, false, AT_MOST_ONCE,
            false, 0);
        MqttPubAckMessage pubRecMessage = new MqttPubAckMessage(fixedHeader, from(messageID));
        sendIfWritableElseDrop(pubRecMessage);
    }

    private void processPubRel(MqttMessage msg) {
        final int messageID = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
        final String clientID = bindedSession.getClientID();
        postOffice.routeCommand(clientID, "PUBREL", () -> {
            checkMatchSessionLoop(clientID);
            executePubRel(messageID);
            return null;
        });
    }

    private void executePubRel(int messageID) {
        bindedSession.receivedPubRelQos2(messageID);
        sendPubCompMessage(messageID);
    }

    void sendPublish(MqttPublishMessage publishMsg) {
        final int packetId = publishMsg.variableHeader().packetId();
        final String topicName = publishMsg.variableHeader().topicName();
        MqttQoS qos = publishMsg.fixedHeader().qosLevel();
        if (LOG.isTraceEnabled()) {
            LOG.trace("Sending PUBLISH({}) message. MessageId={}, topic={}, payload={}", qos, packetId, topicName,
                DebugUtils.payload2Str(publishMsg.payload()));
        } else {
            LOG.debug("Sending PUBLISH({}) message. MessageId={}, topic={}", qos, packetId, topicName);
        }
        sendIfWritableElseDrop(publishMsg);
    }

    void sendIfWritableElseDrop(MqttMessage msg) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("OUT {}", msg.fixedHeader().messageType());
        }
        if (channel.isWritable()) {

            // Sending to external, retain a duplicate. Just retain is not
            // enough, since the receiver must have full control.
            Object retainedDup = msg;
            if (msg instanceof ByteBufHolder) {
                retainedDup = ((ByteBufHolder) msg).retainedDuplicate();
            }

            ChannelFuture channelFuture;
            if (brokerConfig.getBufferFlushMillis() == BrokerConstants.IMMEDIATE_BUFFER_FLUSH) {
                channelFuture = channel.writeAndFlush(retainedDup);
            } else {
                channelFuture = channel.write(retainedDup);
            }
            channelFuture.addListener(FIRE_EXCEPTION_ON_FAILURE);
        }
    }

    public void writabilityChanged() {
        if (channel.isWritable()) {
            LOG.debug("Channel is again writable");
            postOffice.routeCommand(getClientId(), "writabilityChanged", () -> {
                bindedSession.writabilityChanged();
                return null;
            });
        }
    }

    void sendPubAck(int messageID) {
        LOG.trace("sendPubAck for messageID: {}", messageID);
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, false, AT_MOST_ONCE,
                                                  false, 0);
        MqttPubAckMessage pubAckMessage = new MqttPubAckMessage(fixedHeader, from(messageID));
        sendIfWritableElseDrop(pubAckMessage);
    }

    private void sendPubCompMessage(int messageID) {
        LOG.trace("Sending PUBCOMP message messageId: {}", messageID);
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBCOMP, false, AT_MOST_ONCE, false, 0);
        MqttMessage pubCompMessage = new MqttMessage(fixedHeader, from(messageID));
        sendIfWritableElseDrop(pubCompMessage);
    }

    String getClientId() {
        return NettyUtils.clientID(channel);
    }

    String getUsername() {
        return NettyUtils.userName(channel);
    }

    public void sendPublishWithPacketId(Topic topic, MqttQoS qos, ByteBuf payload, boolean retained) {
        final int packetId = nextPacketId();
        MqttPublishMessage publishMsg = createPublishMessage(topic.toString(), qos, payload, packetId, retained);
        sendPublish(publishMsg);
    }

    // TODO move this method in Session
    void sendPublishQos0(Topic topic, MqttQoS qos, ByteBuf payload, boolean retained) {
        MqttPublishMessage publishMsg = createPublishMessage(topic.toString(), qos, payload, 0, retained);
        sendPublish(publishMsg);
    }

    static MqttPublishMessage createRetainedPublishMessage(String topic, MqttQoS qos, ByteBuf message) {
        return createPublishMessage(topic, qos, message, 0, true);
    }

    static MqttPublishMessage createNonRetainedPublishMessage(String topic, MqttQoS qos, ByteBuf message) {
        return createPublishMessage(topic, qos, message, 0, false);
    }

    static MqttPublishMessage createRetainedPublishMessage(String topic, MqttQoS qos, ByteBuf message,
                                                           int messageId) {
        return createPublishMessage(topic, qos, message, messageId, true);
    }

    static MqttPublishMessage createNotRetainedPublishMessage(String topic, MqttQoS qos, ByteBuf message,
                                                              int messageId) {
        return createPublishMessage(topic, qos, message, messageId, false);
    }

    private static MqttPublishMessage createPublishMessage(String topic, MqttQoS qos, ByteBuf message,
                                                           int messageId, boolean retained) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, retained, 0);
        MqttPublishVariableHeader varHeader = new MqttPublishVariableHeader(topic, messageId);
        return new MqttPublishMessage(fixedHeader, varHeader, message);
    }

    public void resendNotAckedPublishes() {
        bindedSession.resendInflightNotAcked();
    }

    int nextPacketId() {
        return lastPacketId.updateAndGet(v -> v == 65535 ? 1 : v + 1);
    }

    @Override
    public String toString() {
        return "MQTTConnection{channel=" + channel + ", connected=" + connected + '}';
    }

    InetSocketAddress remoteAddress() {
        return (InetSocketAddress) channel.remoteAddress();
    }

    public void readCompleted() {
        LOG.debug("readCompleted client CId: {}", getClientId());
        if (getClientId() != null) {
            // TODO drain all messages in target's session in-flight message queue
            queueDrainQueueCommand();
        }
    }

    private void queueDrainQueueCommand() {
        postOffice.routeCommand(getClientId(), "flushQueues", () -> {
            bindedSession.flushAllQueuedMessages();
            return null;
        });
    }

    public void flush() {
        channel.flush();
    }

    private boolean isBoundToSession() {
        return bindedSession != null && bindedSession.isBoundTo(this);
    }

    private boolean isSessionUnbound() {
        return bindedSession != null && bindedSession.isBoundTo(null);
    }

    public void bindSession(Session session) {
        bindedSession = session;
    }

    /**
     * Invoked internally by broker to disconnect a client and close the connection
     * */
    void brokerDisconnect() {
        final MqttMessage disconnectMsg = MqttMessageBuilders.disconnect().build();
        channel.writeAndFlush(disconnectMsg)
            .addListener(ChannelFutureListener.CLOSE);
    }

    void brokerDisconnect(MqttReasonCodes.Disconnect reasonCode) {
        final MqttMessage disconnectMsg = MqttMessageBuilders.disconnect()
            .reasonCode(reasonCode.byteValue())
            .build();
        channel.writeAndFlush(disconnectMsg)
            .addListener(ChannelFutureListener.CLOSE);
    }
}
