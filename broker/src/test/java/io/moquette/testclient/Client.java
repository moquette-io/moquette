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
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
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
    final ClientNettyMQTTHandler handler = new ClientNettyMQTTHandler();
    EventLoopGroup workerGroup;
    Channel m_channel;
    private boolean m_connectionLost;
    private ICallback callback;
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
                    pipeline.addLast("decoder", new MqttDecoder());
                    pipeline.addLast("encoder", MqttEncoder.INSTANCE);
                    pipeline.addLast("handler", handler);
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

    public void connect(String willTestamentTopic, String willTestamentMsg) {
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

    public void connect() {
        MqttConnectMessage connectMessage = MqttMessageBuilders.connect().protocolVersion(MqttVersion.MQTT_3_1_1)
                .clientId("").keepAlive(2) // secs
                .willFlag(false).willQoS(MqttQoS.AT_MOST_ONCE).build();

        doConnect(connectMessage);
    }

    public MqttConnAckMessage connectV5() {
        final MqttMessageBuilders.ConnectBuilder builder = MqttMessageBuilders.connect().protocolVersion(MqttVersion.MQTT_5);
        if (clientId != null) {
            builder.clientId(clientId);
        }
        MqttConnectMessage connectMessage = builder
            .keepAlive(2) // secs
            .willFlag(false)
            .willQoS(MqttQoS.AT_MOST_ONCE)
            .build();

        return doConnect(connectMessage);
    }

    private MqttConnAckMessage doConnect(MqttConnectMessage connectMessage) {
        final CountDownLatch latch = new CountDownLatch(1);
        this.setCallback(msg -> {
            receivedMsg.getAndSet(msg);
            LOG.info("Connect callback invocation, received message {}", msg.fixedHeader().messageType());
            latch.countDown();

            // clear the callback
            setCallback(null);
        });

        this.sendMessage(connectMessage);

        boolean waitElapsed;
        try {
            waitElapsed = !latch.await(2_000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting", e);
        }

        if (waitElapsed) {
            throw new RuntimeException("Cannot receive ConnAck in 2 s");
        }

        final MqttMessage connAckMessage = this.receivedMsg.get();
        if (!(connAckMessage instanceof MqttConnAckMessage)) {
            MqttMessageType messageType = connAckMessage.fixedHeader().messageType();
            throw new RuntimeException("Expected a CONN_ACK message but received " + messageType);
        }
        return (MqttConnAckMessage) connAckMessage;
    }

    public MqttSubAckMessage subscribe(String topic1, MqttQoS qos1, String topic2, MqttQoS qos2) {
        final MqttSubscribeMessage subscribeMessage = MqttMessageBuilders.subscribe()
            .messageId(1)
            .addSubscription(qos1, topic1)
            .addSubscription(qos2, topic2)
            .build();

        return doSubscribeWithAckCasting(subscribeMessage);
    }

    public MqttSubAckMessage subscribe(String topic, MqttQoS qos) {
        final MqttSubscribeMessage subscribeMessage = MqttMessageBuilders.subscribe()
            .messageId(1)
            .addSubscription(qos, topic)
            .build();

        return doSubscribeWithAckCasting(subscribeMessage);
    }

    @NotNull
    private MqttSubAckMessage doSubscribeWithAckCasting(MqttSubscribeMessage subscribeMessage) {
        doSubscribe(subscribeMessage);

        final MqttMessage subAckMessage = this.receivedMsg.get();
        if (!(subAckMessage instanceof MqttSubAckMessage)) {
            MqttMessageType messageType = subAckMessage.fixedHeader().messageType();
            throw new RuntimeException("Expected a SUB_ACK message but received " + messageType);
        }
        return (MqttSubAckMessage) subAckMessage;
    }

    private void doSubscribe(MqttSubscribeMessage subscribeMessage) {
        final CountDownLatch subscribeAckLatch = new CountDownLatch(1);
        this.setCallback(msg -> {
            receivedMsg.getAndSet(msg);
            LOG.debug("Subscribe callback invocation, received message {}", msg.fixedHeader().messageType());
            subscribeAckLatch.countDown();

            // clear the callback
            setCallback(null);
        });

        LOG.debug("Sending SUBSCRIBE message");
        sendMessage(subscribeMessage);
        LOG.debug("Sent SUBSCRIBE message");

        boolean waitElapsed;
        try {
            waitElapsed = !subscribeAckLatch.await(200, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting", e);
        }

        if (waitElapsed) {
            throw new RuntimeException("Cannot receive SubscribeAck in 200 ms");
        }
    }

    public MqttMessage subscribeWithError(String topic, MqttQoS qos) {
        final MqttSubscribeMessage subscribeMessage = MqttMessageBuilders.subscribe()
            .messageId(1)
            .addSubscription(qos, topic)
            .build();

        doSubscribe(subscribeMessage);
        return this.receivedMsg.get();
    }

    public void disconnect() {
        final MqttMessage disconnectMessage = MqttMessageBuilders.disconnect().build();
        sendMessage(disconnectMessage);
    }

    public void shutdownConnection() throws InterruptedException {
        this.workerGroup.shutdownGracefully().sync();
    }

    public void setCallback(ICallback callback) {
        this.callback = callback;
    }

    public void sendMessage(MqttMessage msg) {
        m_channel.writeAndFlush(msg).addListener(FIRE_EXCEPTION_ON_FAILURE);
    }

    public MqttMessage lastReceivedMessage() {
        return this.receivedMsg.get();
    }

    void messageReceived(MqttMessage msg) {
        LOG.info("Received message {}", msg);
        if (this.callback != null) {
            this.callback.call(msg);
        } else {
            receivedMessages.add(msg);
        }
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
}
