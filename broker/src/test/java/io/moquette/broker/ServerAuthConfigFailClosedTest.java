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
package io.moquette.broker;

import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.integration.IntegrationUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A configured-but-failing-to-load authorizator/authenticator class must reject startup, not
 * silently fall through to the permissive PermitAll/AcceptAll default reserved for the
 * nothing-configured case.
 */
public class ServerAuthConfigFailClosedTest {

    private static final String MISSING_CLASS_NAME = "io.moquette.broker.NoSuchAuthorizatorPolicy";

    @TempDir
    Path tempFolder;
    private String dbPath;
    private Server server;

    @BeforeEach
    public void setUp() {
        dbPath = IntegrationUtils.tempH2Path(tempFolder);
    }

    @AfterEach
    public void tearDown() {
        if (server != null) {
            server.stopServer();
        }
    }

    @Test
    public void givenConfiguredAuthorizatorClassFailsToLoadThenStartupIsAborted() {
        Properties props = new Properties(IntegrationUtils.prepareTestProperties(dbPath));
        props.setProperty(IConfig.AUTHORIZATOR_CLASS_NAME, MISSING_CLASS_NAME);
        IConfig config = new MemoryConfig(props);
        server = new Server();

        assertThrows(IllegalArgumentException.class, () -> server.startServer(config));
    }

    @Test
    public void givenConfiguredAuthenticatorClassFailsToLoadThenStartupIsAborted() {
        Properties props = new Properties(IntegrationUtils.prepareTestProperties(dbPath));
        props.setProperty(IConfig.AUTHENTICATOR_CLASS_NAME, MISSING_CLASS_NAME);
        IConfig config = new MemoryConfig(props);
        server = new Server();

        assertThrows(IllegalArgumentException.class, () -> server.startServer(config));
    }

    @Test
    public void givenNoAuthorizatorClassConfiguredThenServerStartsWithTheDefaultPermissivePolicy() throws Exception {
        // No authorizator_class_name / no ACL file configured: the pre-existing, INTENTIONAL default
        // (PermitAll) must be unaffected by this change - the server must start normally. Persistence
        // is disabled here since this test only cares about the auth-init path, not durability.
        Properties props = new Properties();
        props.setProperty(IConfig.PERSISTENCE_ENABLED_PROPERTY_NAME, "false");
        props.setProperty(IConfig.ENABLE_TELEMETRY_NAME, "false");
        IConfig config = new MemoryConfig(props);
        server = new Server();

        server.startServer(config);
    }
}
