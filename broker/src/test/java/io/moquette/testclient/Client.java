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
package io.moquette.testclient;

import io.moquette.BrokerConstants;
import io.moquette.broker.metrics.MQTTMessageLogger;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.util.ReferenceCountUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.channel.ChannelFutureListener.CLOSE_ON_FAILURE;
import static io.netty.channel.ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE;

/**
 * Class used just to send and receive MQTT messages without any protocol login in action, just use
 * the encoder/decoder part.
 */
public class Client {

    public interface ICallback {

        void call(MqttMessage msg);
    }

    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    private static final Duration TIMEOUT_DURATION = Duration.ofMillis(1000);

    final ClientNettyMQTTHandler handler = new ClientNettyMQTTHandler();
    EventLoopGroup workerGroup;
    Channel m_channel;
    private boolean m_connectionLost;
    private String clientId;
    private AtomicReference<MqttMessage> receivedMsg = new AtomicReference<>();
    private final BlockingQueue<MqttMessage> receivedMessages = new LinkedBlockingQueue<>();

    public Client(String host) {
        this(host, BrokerConstants.PORT);
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    public Client(String host, int port) {
        handler.setClient(this);
        workerGroup = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {

                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast("rawcli_decoder", new MqttDecoder());
                    pipeline.addLast("rawcli_encoder", MqttEncoder.INSTANCE);
                    pipeline.addLast("messageLogger", new MQTTMessageLogger());
                    pipeline.addLast("rawcli_handler", handler);
                }
            });

            // Start the client.
            m_channel = b.connect(host, port).sync().channel();
        } catch (Exception ex) {
            LOG.error("Error received in client setup", ex);
            workerGroup.shutdownGracefully();
        }
    }

    public Client clientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public void connect(String willTestamentTopic, String willTestamentMsg) throws InterruptedException {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(
                MqttMessageType.CONNECT,
                false,
                MqttQoS.AT_MOST_ONCE,
                false,
                0);
        MqttConnectVariableHeader mqttConnectVariableHeader = new MqttConnectVariableHeader(
                MqttVersion.MQTT_3_1.protocolName(),
                MqttVersion.MQTT_3_1.protocolLevel(),
                false,
                false,
                false,
                MqttQoS.AT_MOST_ONCE.value(),
                true,
                true,
                2);
        MqttConnectPayload mqttConnectPayload = new MqttConnectPayload(
                this.clientId,
                willTestamentTopic,
                willTestamentMsg.getBytes(Charset.forName("UTF-8")),
                null,
                null);
        MqttConnectMessage connectMessage = new MqttConnectMessage(
                mqttFixedHeader,
                mqttConnectVariableHeader,
                mqttConnectPayload);

        doConnect(connectMessage);
    }

    public MqttConnAckMessage connect() throws InterruptedException {
        MqttConnectMessage connectMessage = MqttMessageBuilders.connect().protocolVersion(MqttVersion.MQTT_3_1_1)
                .clientId("").keepAlive(2) // secs
                .willFlag(false).willQoS(MqttQoS.AT_MOST_ONCE).build();

        return doConnect(connectMessage);
    }

    public MqttConnAckMessage connectV5() throws InterruptedException {
        return connectV5(2, BrokerConstants.INFLIGHT_WINDOW_SIZE);
    }

    public MqttConnAckMessage connectV5WithReceiveMaximum(int receiveMaximumInflight) throws InterruptedException {
        return connectV5(2, receiveMaximumInflight);
    }

    @NotNull
    public MqttConnAckMessage connectV5(int keepAliveSecs, int receiveMaximumInflight) throws InterruptedException {
        final MqttMessageBuilders.ConnectBuilder builder = MqttMessageBuilders.connect().protocolVersion(MqttVersion.MQTT_5);
        if (clientId != null) {
            builder.clientId(clientId);
        }

        final MqttProperties connectProperties = new MqttProperties();
        MqttProperties.IntegerProperty receiveMaximum = new MqttProperties.IntegerProperty(
            MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM.value(),
            receiveMaximumInflight);
        connectProperties.add(receiveMaximum);

        MqttConnectMessage connectMessage = builder
            .keepAlive(keepAliveSecs) // secs
            .willFlag(false)
            .willQoS(MqttQoS.AT_MOST_ONCE)
            .properties(connectProperties)
            .build();

        return doConnect(connectMessage);
    }

    private MqttConnAckMessage doConnect(MqttConnectMessage connectMessage) throws InterruptedException {
        this.sendMessage(connectMessage);

        final MqttMessage connAckMessage = this.receiveNextMessage(Duration.ofMillis(2_000));
        if (connAckMessage == null) {
            throw new RuntimeException("Cannot receive ConnAck in 2 s");
        }
        if (!(connAckMessage instanceof MqttConnAckMessage)) {
            MqttMessageType messageType = connAckMessage.fixedHeader().messageType();
            throw new RuntimeException("Expected a CONN_ACK message but received " + messageType);
        }
        return (MqttConnAckMessage) connAckMessage;
    }

    public MqttSubAckMessage subscribe(String topic1, MqttQoS qos1, String topic2, MqttQoS qos2) throws InterruptedException {
        final MqttSubscribeMessage subscribeMessage = MqttMessageBuilders.subscribe()
            .messageId(1)
            .addSubscription(qos1, topic1)
            .addSubscription(qos2, topic2)
            .build();

        return doSubscribeWithAckCasting(subscribeMessage, TIMEOUT_DURATION);
    }

    public MqttSubAckMessage subscribe(String topic, MqttQoS qos) throws InterruptedException {
        final MqttSubscribeMessage subscribeMessage = MqttMessageBuilders.subscribe()
            .messageId(1)
            .addSubscription(qos, topic)
            .build();

        return doSubscribeWithAckCasting(subscribeMessage, TIMEOUT_DURATION);
    }

    public MqttSubAckMessage subscribeWithIdentifier(String topic, MqttQoS qos, int subscriptionIdentifier) throws InterruptedException {
        return subscribeWithIdentifier(topic, qos, subscriptionIdentifier, TIMEOUT_DURATION);
    }

    @NotNull
    public MqttSubAckMessage subscribeWithIdentifier(String topic, MqttQoS qos, int subscriptionIdentifier,
                                                     Duration timeout) throws InterruptedException {
        MqttProperties subProps = new MqttProperties();
        subProps.add(new MqttProperties.IntegerProperty(
            MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER.value(),
            subscriptionIdentifier));

        final MqttSubscribeMessage subscribeMessage = MqttMessageBuilders.subscribe()
            .messageId(1)
            .addSubscription(qos, topic)
            .properties(subProps)
            .build();

        return doSubscribeWithAckCasting(subscribeMessage, timeout);
    }

    @NotNull
    private MqttSubAckMessage doSubscribeWithAckCasting(MqttSubscribeMessage subscribeMessage, Duration timeout) throws InterruptedException {
        doSubscribe(subscribeMessage);

        final MqttMessage subAckMessage = this.receiveNextMessage(timeout);
        if (!(subAckMessage instanceof MqttSubAckMessage)) {
            MqttMessageType messageType = subAckMessage.fixedHeader().messageType();
            throw new RuntimeException("Expected a SUB_ACK message but received " + messageType);
        }
        return (MqttSubAckMessage) subAckMessage;
    }

    private void doSubscribe(MqttSubscribeMessage subscribeMessage) {
        LOG.debug("Sending SUBSCRIBE message");
        sendMessage(subscribeMessage);
        LOG.debug("Sent SUBSCRIBE message");
    }

    public void publish(MqttPublishMessage publishMessage) {
        LOG.debug("Sending PUBLISH message");
        sendMessage(publishMessage);
        LOG.debug("Sent PUBLISH message");
    }

    public MqttMessage subscribeWithError(String topic, MqttQoS qos) {
        final MqttSubscribeMessage subscribeMessage = MqttMessageBuilders.subscribe()
            .messageId(1)
            .addSubscription(qos, topic)
            .build();

        doSubscribe(subscribeMessage);
        try {
            MqttMessage mqttMessage = this.receiveNextMessage(TIMEOUT_DURATION);
            if (mqttMessage == null) {
                throw new RuntimeException("Cannot receive SubscribeAck in " + TIMEOUT_DURATION);
            }
            return mqttMessage;
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting", e);
        }
    }

    public void disconnect() {
        final MqttMessage disconnectMessage = MqttMessageBuilders.disconnect().build();
        sendMessage(disconnectMessage);
        // release all queued publishes
        for (MqttMessage msg : receivedMessages) {
            ReferenceCountUtil.release(msg);
        }
    }

    public void shutdownConnection() throws InterruptedException {
        this.workerGroup.shutdownGracefully().sync();
    }

    public void sendMessage(MqttMessage msg) {
        m_channel.writeAndFlush(msg).addListener(FIRE_EXCEPTION_ON_FAILURE);
    }

    public MqttMessage lastReceivedMessage() {
        return this.receivedMsg.get();
    }

    void messageReceived(MqttMessage msg) {
        LOG.debug("Received message {}", msg);
        receivedMessages.add(msg);
    }

    public boolean hasReceivedMessages() {
        return !receivedMessages.isEmpty();
    }

    void setConnectionLost(boolean status) {
        m_connectionLost = status;
    }

    public boolean isConnectionLost() {
        return m_connectionLost;
    }

    public Optional<MqttMessage> nextQueuedMessage() {
        if (receivedMessages.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(receivedMessages.poll());
    }

    public MqttMessage receiveNextMessage(Duration waitTime) throws InterruptedException {
        return receivedMessages.poll(waitTime.toMillis(), TimeUnit.MILLISECONDS);
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    public void close() throws InterruptedException {
        // Wait until the connection is closed.
        m_channel.closeFuture().sync().addListener(CLOSE_ON_FAILURE);
        if (workerGroup == null) {
            throw new IllegalStateException("Invoked close on an Acceptor that wasn't initialized");
        }
        workerGroup.shutdownGracefully();
    }

    public boolean isConnected() {
        return m_channel.isActive();
    }
}
