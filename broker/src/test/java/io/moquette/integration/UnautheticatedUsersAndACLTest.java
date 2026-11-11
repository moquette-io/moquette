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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A broker configured without any authenticator (no password_file, no authenticator_class) but with an ACL
 * file that contains a "user" section must refuse to start. Otherwise the AcceptAllAuthenticator would let
 * any client claim the ACL username with an arbitrary password and obtain that user's grants.
 */
public class UnautheticatedUsersAndACLTest {

    @TempDir
    Path tempFolder;

    private Server server;

    @AfterEach
    public void tearDown() {
        if (server != null) {
            server.stopServer();
        }
    }

    @Test
    public void brokerWithoutPasswordFileAndAclWithUserSectionFailsToStart() throws IOException {
        Path aclFile = tempFolder.resolve("acl.conf");
        Files.write(aclFile, ("user bob\n"
            + "topic readwrite tenants/bob/private\n").getBytes(StandardCharsets.UTF_8));

        String dbPath = IntegrationUtils.tempH2Path(tempFolder);
        Properties props = IntegrationUtils.prepareTestProperties(dbPath);
        props.put(IConfig.ALLOW_ANONYMOUS_PROPERTY_NAME, "false");
        props.put(IConfig.ACL_FILE_PROPERTY_NAME, aclFile.toAbsolutePath().toString());
        // no PASSWORD_FILE_PROPERTY_NAME and no AUTHENTICATOR_CLASS_NAME

        server = new Server();
        assertThrows(IllegalArgumentException.class, () -> server.startServer(new MemoryConfig(props)),
            "Broker must refuse to start when the ACL defines users but no authenticator is configured");
    }
}
