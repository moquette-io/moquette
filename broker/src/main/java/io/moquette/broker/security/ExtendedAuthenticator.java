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

public interface ExtendedAuthenticator {
    
    class AuthenticatorResult {

        private final boolean failure;
        private final boolean success;
        private final byte[] data;

        public static AuthenticatorResult failure() {
            return new AuthenticatorResult(true, false, null);
        }

        public static AuthenticatorResult success() {
            return new AuthenticatorResult(false, true, null);
        }

        public static AuthenticatorResult continueFlow(byte[] data) {
            return new AuthenticatorResult(false, false, data);
        }
        
        private AuthenticatorResult(boolean failure, boolean success, byte[] data) {
            this.failure = failure;
            this.success = success;
            this.data = data;
        }
        
        public boolean isFailure() {
            return failure;
        }

        public boolean isCompleted() {
            return success;
        }

        public byte[] getReplyData() {
            return data;
        }
    }
    
    /**
     * Name of the authenticator corresponding to Authentication Method property.
     * */
    String name();
    
    /**
     * Executes one step of the authentication flow and return a result which could be:
     * <ul>
     *   <li>success</li>
     *   <li>failure</li>
     *   <li>continue</li>
     * </ul>
     * */
    AuthenticatorResult processStep(byte[] value);
}
