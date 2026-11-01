/*
 *
 *  * Copyright (c) 2012-2024 The original author or authors
 *  * ------------------------------------------------------
 *  * All rights reserved. This program and the accompanying materials
 *  * are made available under the terms of the Eclipse Public License v1.0
 *  * and Apache License v2.0 which accompanies this distribution.
 *  *
 *  * The Eclipse Public License is available at
 *  * http://www.eclipse.org/legal/epl-v10.html
 *  *
 *  * The Apache License v2.0 is available at
 *  * http://www.opensource.org/licenses/apache2.0.php
 *  *
 *  * You may elect to redistribute this code under either of these licenses.
 *
 */

package io.moquette.broker.security;

import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Registry of ExtendedAuthenticator implementations.
 * Lookups implementation using Java ServiceLoader facility.
 * */
public class AuthenticatorRegistry {
    
    public static final ExtendedAuthenticator FAIL_AUTHENTICATOR = new ExtendedAuthenticator() {
        @Override
        public String name() {
            return "CONTINUOUS_FAIL";
        }

        @Override
        public AuthenticatorResult processStep(byte[] value) {
            return AuthenticatorResult.failure();
        }
    };

    private final ConcurrentMap<String, ExtendedAuthenticatorProvider> factories = new ConcurrentHashMap<>();

    public AuthenticatorRegistry() {
        for (ExtendedAuthenticatorProvider factory : ServiceLoader.load(ExtendedAuthenticatorProvider.class)) {
            factories.put(factory.authenticationMethod(), factory);
        }
    }

    /**
     * Create an extended authenticator by method, if not found return a default failing implementation.
     * */
    public ExtendedAuthenticator createByMethodName(String authenticationMethod) {
        // Must create a new fresh instance, because is statefull and each instance
        // is used by one MQTTConnection
        ExtendedAuthenticatorProvider factory = factories.get(authenticationMethod);
        if (factory != null) {
            return factory.createAuthenticator();
        }
        return FAIL_AUTHENTICATOR;
    }

    public boolean isMethodSupported(String authenticationMethod) {
        return factories.containsKey(authenticationMethod);
    }
}
