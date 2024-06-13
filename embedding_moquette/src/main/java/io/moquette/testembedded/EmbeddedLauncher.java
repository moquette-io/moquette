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
package io.moquette.testembedded;

import io.moquette.broker.ClientDescriptor;
import io.moquette.broker.Server;
import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.InterceptHandler;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.broker.config.ClasspathResourceLoader;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.IResourceLoader;
import io.moquette.broker.config.ResourceLoaderConfig;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Simple example of how to embed the broker in another project
 * */
public final class EmbeddedLauncher {

    private class PublisherListener extends AbstractInterceptHandler {

        @Override
        public String getID() {
            return "EmbeddedLauncherPublishListener";
        }

        @Override
        public void onPublish(InterceptPublishMessage msg) {
            final String decodedPayload = msg.getPayload().toString(UTF_8);
            System.out.println("Received on topic: " + msg.getTopicName() + " content: " + decodedPayload);
            if ("/command".equals(msg.getTopicName())) {
                switch (decodedPayload) {
                    case "exit":
                        System.out.println("EXITING broker by /command exit");
                        shutdown();
                        return;
                    case "list_clients":
                        listClients();
                        return;
                }
            }
        }
        
        @Override
        public void onSessionLoopError(Throwable error) {
            System.out.println("Session event loop reported error: " + error);
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        final EmbeddedLauncher launcher = new EmbeddedLauncher();
        launcher.start();
    }

    private Server mqttBroker;

    private EmbeddedLauncher() {
    }

    private void start() throws IOException, InterruptedException {
        IResourceLoader classpathLoader = new ClasspathResourceLoader();
        final IConfig classPathConfig = new ResourceLoaderConfig(classpathLoader);

        mqttBroker = new Server();
        List<? extends InterceptHandler> userHandlers = Collections.singletonList(new PublisherListener());
        mqttBroker.startServer(classPathConfig, userHandlers);

        System.out.println("Broker started press [CTRL+C] to stop");

        //Bind a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));

        Thread.sleep(20000);
        internalPublish("Hello World!!");
    }

    private void internalPublish(String messageText) {
        System.out.println("Before self publish");
        MqttPublishMessage message = MqttMessageBuilders.publish()
            .topicName("/exit")
            .retained(true)
//        qos(MqttQoS.AT_MOST_ONCE);
//        qQos(MqttQoS.AT_LEAST_ONCE);
            .qos(MqttQoS.EXACTLY_ONCE)
            .payload(Unpooled.copiedBuffer(messageText.getBytes(UTF_8)))
            .build();

        mqttBroker.internalPublish(message, "INTRLPUB");
        System.out.println("After self publish");
    }

    private void shutdown() {
        listClients();

        System.out.println("Stopping broker");
        mqttBroker.stopServer();
        System.out.println("Broker stopped");
    }

    private void listClients() {
        final Collection<ClientDescriptor> connectedClients = mqttBroker.listConnectedClients();
        if (connectedClients.isEmpty()) {
            System.out.println("No connected clients");
        }
        for (ClientDescriptor client : connectedClients) {
            System.out.println(client);
        }
    }

}
