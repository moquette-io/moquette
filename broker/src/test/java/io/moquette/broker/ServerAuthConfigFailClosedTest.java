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

package io.moquette.broker;

import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.broker.security.IAuthenticator;
import io.moquette.broker.security.IAuthorizatorPolicy;
import io.moquette.broker.security.PermitAllAuthorizatorPolicy;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A configured-but-failing-to-load authorizator/authenticator class must abort startup, not
 * silently fall through to the permissive PermitAll/AcceptAll default reserved for the
 * nothing-configured case. See MOQ-2026-0036.
 */
public class ServerAuthConfigFailClosedTest {

    private static final String MISSING_CLASS_NAME = "io.moquette.broker.NoSuchAuthorizatorPolicy";

    private Object invokePrivate(String methodName, Class<?> paramType, Object first, IConfig config)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Server server = new Server();
        Method m = Server.class.getDeclaredMethod(methodName, paramType, IConfig.class);
        m.setAccessible(true);
        try {
            return m.invoke(server, first, config);
        } catch (InvocationTargetException ite) {
            // unwrap so assertThrows sees the real IllegalArgumentException, not the reflection wrapper
            if (ite.getCause() instanceof RuntimeException) {
                throw (RuntimeException) ite.getCause();
            }
            throw ite;
        }
    }

    @Test
    public void givenConfiguredAuthorizatorClassFailsToLoadThenStartupIsAborted() {
        Properties props = new Properties();
        props.setProperty(IConfig.AUTHORIZATOR_CLASS_NAME, MISSING_CLASS_NAME);
        IConfig config = new MemoryConfig(props);

        assertThrows(IllegalArgumentException.class,
            () -> invokePrivate("initializeAuthorizatorPolicy", IAuthorizatorPolicy.class, null, config));
    }

    @Test
    public void givenConfiguredAuthenticatorClassFailsToLoadThenStartupIsAborted() {
        Properties props = new Properties();
        props.setProperty(IConfig.AUTHENTICATOR_CLASS_NAME, MISSING_CLASS_NAME);
        IConfig config = new MemoryConfig(props);

        assertThrows(IllegalArgumentException.class,
            () -> invokePrivate("initializeAuthenticator", IAuthenticator.class, null, config));
    }

    @Test
    public void givenNoAuthorizatorClassConfiguredThenTheDefaultPermissivePolicyIsStillUsed() throws Exception {
        // No authorizator_class_name / no ACL file configured: the pre-existing, INTENTIONAL default
        // (PermitAll) must be unaffected by this change.
        IConfig config = new MemoryConfig(new Properties());

        Object policy = invokePrivate("initializeAuthorizatorPolicy", IAuthorizatorPolicy.class, null, config);
        assertEquals(PermitAllAuthorizatorPolicy.class, policy.getClass());
    }
}
