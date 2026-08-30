/*
 * Copyright (c) 2012-2026 The original author or authors
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
package io.moquette.integration;

import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression test for the buffer-leak in {@code MQTTConnection.processPublish}
 * triggered when a file-based ACL is configured and the client sends a PUBLISH
 * with a malformed topic name.
 *
 * <h3>Root cause</h3>
 * <ol>
 *   <li>{@code processPublish} builds a {@code new Topic(topicName)}.  An empty
 *       topic name causes {@code Topic.parseTopic()} to throw a
 *       {@code ParseException} (MQTT-4.7.3-1: topic must be ≥ 1 character);
 *       the token list stays {@code null} and {@code isValid()} returns
 *       {@code false}.  Note: Netty 4.1's {@code MqttDecoder} only rejects
 *       topics that contain {@code '+'} or {@code '#'}; an empty topic passes
 *       the decoder and reaches {@code processPublish}.</li>
 *   <li>The guard {@code if (!topic.isValid()) { dropConnection(); }} is missing
 *       an early {@code return}, so execution falls through to
 *       {@code Utils.retain(msg, BT_PUB_IN)}, incrementing the
 *       {@code ByteBuf} reference count from 1 to 2.</li>
 *   <li>The {@code finally} block in {@code NewNettyMQTTHandler.channelRead}
 *       decrements the count back to 1, leaving one retained reference owned by
 *       the session-event-loop lambda.</li>
 *   <li>Inside the session event-loop {@code receivedPublishQos0} recreates the
 *       same {@code Topic("")} and calls {@code authorizator.canWrite(topic, …)}.
 *       The ACL file parser produces an {@code AuthorizationsCollector} whose
 *       {@code matchACL} calls {@code topic.match(aclTopic)}.  Because the
 *       publish topic has a {@code null} token list, the match loop hits
 *       {@code msgTokens.size()} → {@code NullPointerException}.</li>
 *   <li>The NPE escapes the lambda without ever calling
 *       {@code Utils.release(msg, …)}, so the reference count stays at 1 and
 *       the server-side pooled read buffer is never returned to the pool →
 *       permanent leak.</li>
 * </ol>
 *
 * <h3>Test strategy</h3>
 * The test opens 1 024 one-shot connections, each sending a single malformed
 * PUBLISH (empty topic name) with an {@value #PAYLOAD_BYTES}-byte payload.
 * It snapshots {@link PooledByteBufAllocator#DEFAULT}'s {@code usedDirectMemory}
 * and {@code usedHeapMemory} before and after, then asserts that the allocator
 * reports at least {@value #NUM_PUBLISHES} × {@value #PAYLOAD_BYTES} bytes
 * (= 8 MiB) more memory in use after the run.
 *
 * <p>The ACL is loaded from {@code src/test/resources/invalid_topic.acl} which
 * grants write permission on the exact topic {@code /finance/ibm}.  A
 * specific (non-wildcard) ACL topic is required to expose the NPE: a wildcard
 * {@code #} ACL would short-circuit via the MULTI branch and never reach the
 * code path that dereferences the null token list.
 */
public class PublishMalformedTopicBufferLeakIT {

    private static final Logger LOG = LoggerFactory.getLogger(PublishMalformedTopicBufferLeakIT.class);

    /** Total number of malformed PUBLISH messages to fire. */
    static final int NUM_PUBLISHES = 1024;

    /**
     * Payload size per message in bytes.
     * 8 KiB × 1 024 messages = 8 MiB expected total pool leak.
     */
    static final int PAYLOAD_BYTES = 8 * 1024;

    /**
     * Malformed topic: an empty string violates MQTT-4.7.3-1 (topic must be ≥ 1
     * character), so {@code Topic.parseTopic()} throws a {@code ParseException}
     * and the token list stays {@code null}.  Netty 4.1's {@code MqttDecoder}
     * only rejects topics containing {@code '+'} or {@code '#'}, so the empty
     * topic is decoded normally and reaches {@code processPublish}.
     */
    static final String MALFORMED_TOPIC = "";

    @TempDir
    Path tempFolder;

    private Server         server;
    private EventLoopGroup eventLoopGroup;
    private Bootstrap      bootstrap;

    @BeforeEach
    void setUp() throws Exception {
        // Point moquette.path to the test-resources root so that
        // FileResourceLoader resolves "invalid_topic.acl" from there.
        String resourcesRoot = getClass().getResource("/").getPath();
        System.setProperty("moquette.path", resourcesRoot);

        Properties props = IntegrationUtils.prepareTestProperties(IntegrationUtils.tempH2Path(tempFolder));
        props.setProperty(IConfig.ACL_FILE_PROPERTY_NAME, "invalid_topic.acl");
        // Raise the per-message cap so the 8 KiB PUBLISH payload is accepted by
        // MqttDecoder and reaches processPublish (default is 8092 bytes).
        props.setProperty(IConfig.NETTY_MAX_BYTES_PROPERTY_NAME, "65535");

        server = new Server();
        server.startServer(new MemoryConfig(props));

        // Shared event-loop group reused across all 1 024 connections to avoid
        // the overhead of creating/destroying a thread pool per connection.
        eventLoopGroup = new NioEventLoopGroup(2);

        bootstrap = new Bootstrap()
            .group(eventLoopGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline()
                      .addLast("mqtt-decoder", new MqttDecoder())
                      .addLast("mqtt-encoder", MqttEncoder.INSTANCE);
                }
            });
    }

    @AfterEach
    void tearDown() throws Exception {
        eventLoopGroup.shutdownGracefully(0, 200, TimeUnit.MILLISECONDS).sync();
        server.stopServer();
    }

    @Test
    void malformedPublishWithFileAclLeaksByteBufs() throws Exception {
        MetricsData before = MetricsData.snapshot(PooledByteBufAllocator.DEFAULT.metric());
        LOG.info("Pool snapshot BEFORE: usedDirect={} B  usedHeap={} B",
                 before.usedDirectMemory(), before.usedHeapMemory());

        // 8 KiB payload; reuse the same byte array – content is irrelevant.
        // Unpooled.wrappedBuffer does not draw from PooledByteBufAllocator.DEFAULT
        // so it does not inflate the "before" baseline.
        byte[] payload = new byte[PAYLOAD_BYTES];

        // ── Send 1 024 malformed PUBLISH messages, one per connection ───────────────
        for (int i = 0; i < NUM_PUBLISHES; i++) {
            Channel ch = openAndConnect("leaker-" + i);

            MqttPublishMessage publish = MqttMessageBuilders.publish()
                .topicName(MALFORMED_TOPIC)
                .qos(MqttQoS.AT_MOST_ONCE)
                .retained(false)
                .payload(Unpooled.wrappedBuffer(payload))
                .build();
            ch.writeAndFlush(publish);

            // The broker calls dropConnection() upon receiving the empty-topic
            // PUBLISH; wait for the resulting TCP FIN.  1 s is generous for a
            // loopback connection.
            boolean serverClosedFirst = ch.closeFuture().await(1_000, TimeUnit.MILLISECONDS);
            if (!serverClosedFirst) {
                LOG.warn("Connection {} was not closed by the server within 1 s", i);
            }
            ch.close().sync();
        }

        // ── Allow session event-loops to drain all queued lambdas ─────────────────
        // Each lambda throws an NPE (the bug) without releasing finalMsg; the
        // session event-loop catches the exception and continues.  3 seconds is
        // well above what 1 024 fast-failing lambdas need to complete.
        Thread.sleep(3_000);

        // ── Snapshot the pool size after the run ───────────────────────────────────
        MetricsData after = MetricsData.snapshot(PooledByteBufAllocator.DEFAULT.metric());
        LOG.info("Pool snapshot AFTER:  usedDirect={} B  usedHeap={} B",
                 after.usedDirectMemory(), after.usedHeapMemory());

        long leakedBytes = after.leakedBytes(before);
        long expectedLeak = (long) NUM_PUBLISHES * PAYLOAD_BYTES; // 8 MiB
        LOG.info("Measured leaked bytes: {}  expected >= {}", leakedBytes, expectedLeak);

        assertTrue(
            leakedBytes >= expectedLeak,
            String.format(
                "Expected a Netty pool leak of at least %d bytes (%d MiB = %d messages × %d KiB) "
                    + "but measured only %d bytes.\n"
                    + "usedDirect: %d → %d  usedHeap: %d → %d\n"
                    + "This assertion should FAIL once the processPublish early-return fix is applied.",
                expectedLeak, expectedLeak / (1024 * 1024),
                NUM_PUBLISHES, PAYLOAD_BYTES / 1024,
                leakedBytes,
                before.usedDirectMemory(), after.usedDirectMemory(),
                before.usedHeapMemory(), after.usedHeapMemory()));
    }

    /**
     * Opens a raw TCP connection to the broker and immediately writes a minimal
     * MQTT CONNECT frame.  No CONNACK is awaited: because CONNECT and the
     * subsequent PUBLISH travel on the same TCP stream the broker processes them
     * in order – the session is fully established before the PUBLISH lambda runs
     * on the session event-loop.
     */
    private Channel openAndConnect(String clientId) throws InterruptedException {
        Channel ch = bootstrap.connect("localhost", 1883).sync().channel();

        ch.writeAndFlush(
            MqttMessageBuilders.connect()
                .protocolVersion(MqttVersion.MQTT_3_1_1)
                .clientId(clientId)
                .cleanSession(true)
                .keepAlive(60)
                .build());

        return ch;
    }
}
