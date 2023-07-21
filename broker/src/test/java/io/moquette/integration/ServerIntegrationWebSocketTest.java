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

package io.moquette.integration;

import io.moquette.broker.Server;
import io.moquette.BrokerConstants;
import io.moquette.broker.config.FluentConfig;
import io.moquette.broker.config.MemoryConfig;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import io.moquette.broker.config.IConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test to check the function of Moquette with a WebSocket channel.
 */
public class ServerIntegrationWebSocketTest {

    private static final Logger LOG = LoggerFactory.getLogger(ServerIntegrationWebSocketTest.class);

    Server m_server;
    WebSocketClient client;
    IConfig m_config;

    @TempDir
    Path tempFolder;

    protected void startServer(String dbPath) throws IOException {
//        m_server = new Server();
//        m_config = new FluentConfig()
//            .dataPath(dbPath)
//            .enablePersistence()
//            .disableTelemetry()
//            .websocketPort(BrokerConstants.WEBSOCKET_PORT)
//            .build();
//        m_server.startServer(m_config);

        m_server = new Server()
            .withConfig()
            .dataPath(dbPath)
            .enablePersistence()
            .disableTelemetry()
            .websocketPort(BrokerConstants.WEBSOCKET_PORT)
            .startServer();
    }

    @BeforeEach
    public void setUp() throws Exception {
        String dbPath = IntegrationUtils.tempH2Path(tempFolder);
        startServer(dbPath);
        client = new WebSocketClient();
    }

    @AfterEach
    public void tearDown() throws Exception {
        client.stop();
        m_server.stopServer();
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    @Test
    public void checkPlainConnect() throws Exception {
        LOG.info("*** checkPlainConnect ***");
        String destUri = "ws://localhost:" + BrokerConstants.WEBSOCKET_PORT + BrokerConstants.WEBSOCKET_PATH;

        MQTTWebSocket socket = new MQTTWebSocket();
        client.start();
        URI echoUri = new URI(destUri);
        ClientUpgradeRequest request = new ClientUpgradeRequest();
        client.connect(socket, echoUri, request);
        LOG.info("Connecting to : {}", echoUri);
        boolean connected = socket.awaitConnected(4, TimeUnit.SECONDS);
        LOG.info("Connected was : {}", connected);

        assertTrue(connected);
    }
}
