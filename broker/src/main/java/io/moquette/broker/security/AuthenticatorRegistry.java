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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Registry of ExtendedAuthenticator implementations.
 * */
public class AuthenticatorRegistry {
    
    private final ConcurrentMap<String, ExtendedAuthenticator> authenticators = new ConcurrentHashMap<>();
    
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
    
    /**
     * Create an extended authenticator by method, if not found return a default failing implementation.
     * */
    public ExtendedAuthenticator createByMethodName(String authenticationMethod) {
        // TODO this must create a new fresh instance, because it's stored in the MQTTConnection and 
        // could be stateful.
        return authenticators.getOrDefault(authenticationMethod, FAIL_AUTHENTICATOR);
    }
}
