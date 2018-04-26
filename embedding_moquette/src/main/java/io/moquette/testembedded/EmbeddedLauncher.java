/*
 * Copyright (c) 2012-2017 The original author or authors
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
package io.moquette.testembedded;

import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.InterceptHandler;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.server.Server;
import io.moquette.server.config.ClasspathResourceLoader;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.IResourceLoader;
import io.moquette.server.config.ResourceLoaderConfig;
import io.moquette.server.netty.NettyUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.io.IOException;
import java.util.Collections;
import java.util.List;


final class EmbeddedLauncher {

    static class PublisherListener extends AbstractInterceptHandler {

        @Override
        public String getID() {
            return "EmbeddedLauncherPublishListener";
        }

        @Override
        public void onPublish(InterceptPublishMessage msg) {
            byte[] bytes = new byte[msg.getPayload().readableBytes()];
            msg.getPayload().getBytes(msg.getPayload().readerIndex(), bytes);
            System.out.println("Received on topic: " + msg.getTopicName() + " content: " + new String(bytes));
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        IResourceLoader classpathLoader = new ClasspathResourceLoader();
        final IConfig classPathConfig = new ResourceLoaderConfig(classpathLoader);

        // 1. Start a broker.
        final Server mqttBroker = new Server();
        List<? extends InterceptHandler> userHandlers = Collections.singletonList(new PublisherListener());
        mqttBroker.startServer(classPathConfig, userHandlers);

        System.out.println("Broker started press [CTRL+C] to stop");
        // 2. Bind a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Stopping broker");
            mqttBroker.stopServer();
            System.out.println("Broker stopped");
        }));

        // 3. construct publish message.
        Thread.sleep(10000);
        System.out.println("Before self publish");
        MqttPublishMessage message = MqttMessageBuilders.publish()
            .topicName("/exit")
            .retained(true)
            .qos(MqttQoS.AT_MOST_ONCE)
            .payload(Unpooled.copiedBuffer("Hello World!!".getBytes()))
            .build();

        // 4. construct local channel: fake channel.
        EmbeddedChannel m_channel = new EmbeddedChannel();
        NettyUtils.clientID(m_channel, "fake channel");
        NettyUtils.cleanSession(m_channel, false);

        // 5. publish the message.
        mqttBroker.getProcessor().processPublish(m_channel, message);

        System.out.println("After self publish");

        // 6. Note: the below usage won't call the PublisherListener InterceptHandler
        // mqttBroker.internalPublish(message, "INTRLPUB");
    }

    private EmbeddedLauncher() {
    }
}
