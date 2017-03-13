    /*
 * Copyright (c) 2012-2017 The original author or authorsgetRockQuestions()
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
    import io.moquette.server.netty.MessageBuilder;
    import io.netty.handler.codec.mqtt.MqttPublishMessage;
    import io.netty.handler.codec.mqtt.MqttQoS;

    import java.io.IOException;
    import java.util.List;

    import static java.util.Arrays.asList;

    public class EmbeddedLauncher {
    static class PublisherListener extends AbstractInterceptHandler {
        
        @Override
        public String getID() {
            return "EmbeddedLauncherPublishListener";
        }

        @Override
        public void onPublish(InterceptPublishMessage msg) {
            System.out.println("Received on topic: " + msg.getTopicName() + " content: " + new String(msg.getPayload().array()));
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        IResourceLoader classpathLoader = new ClasspathResourceLoader();
        final IConfig classPathConfig = new ResourceLoaderConfig(classpathLoader);

        final Server mqttBroker = new Server();
        List<? extends InterceptHandler> userHandlers = asList(new PublisherListener());
        mqttBroker.startServer(classPathConfig, userHandlers);

        System.out.println("Broker started press [CTRL+C] to stop");
        //Bind  a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Stopping broker");
                mqttBroker.stopServer();
                System.out.println("Broker stopped");
            }
        });

        Thread.sleep(20000);
        System.out.println("Before self publish");
        MqttPublishMessage message = MessageBuilder.publish()
                .topicName("/exit")
                .retained(true)
//        qos(MqttQoS.AT_MOST_ONCE);
//        qQos(MqttQoS.AT_LEAST_ONCE);
                .qos(MqttQoS.EXACTLY_ONCE)
                .payload("Hello World!!".getBytes())
                .build();
        mqttBroker.internalPublish(message, "INTRLPUB");
        System.out.println("After self publish");
    }
}