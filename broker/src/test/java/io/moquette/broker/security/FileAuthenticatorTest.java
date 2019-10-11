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

package io.moquette.broker.security;

import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import org.junit.Test;

import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("deprecation")
public class FileAuthenticatorTest {

    private IConfig config = new MemoryConfig(new Properties());

    @Test
    public void loadPasswordFile_verifyValid() {
        String file = getClass().getResource("/password_file.conf").getPath();
        IAuthenticator auth = new FileAuthenticator(null, file, config);

        assertTrue(auth.checkValid(null, "testuser", "passwd".getBytes(UTF_8)).join());
    }

    @Test
    public void loadPasswordFile_verifyInvalid() {
        String file = getClass().getResource("/password_file.conf").getPath();
        IAuthenticator auth = new FileAuthenticator(null, file, config);

        assertFalse(auth.checkValid(null, "testuser2", "passwd".getBytes(UTF_8)).join());
    }

    @Test
    public void loadPasswordFile_verifyDirectoryRef() {
        IAuthenticator auth = new FileAuthenticator("", "", config);

        assertFalse(auth.checkValid(null, "testuser2", "passwd".getBytes(UTF_8)).join());
    }

}
