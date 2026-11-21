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

package io.moquette.broker.security.scram;

import com.ongres.scram.common.ScramMechanism;
import io.moquette.broker.security.ExtendedAuthenticator;
import io.moquette.broker.security.ExtendedAuthenticatorProvider;

import java.nio.file.Path;
import java.nio.file.Paths;

public class ScramSha256Provider implements ExtendedAuthenticatorProvider {

    private final ScramUserLookup scramUserLookup;

    public ScramSha256Provider() {
        Path pathToScramUserFile = Paths.get(System.getProperty("moquette.path", "."), "config", "users.yaml");
        scramUserLookup = new ScramUserLookup(pathToScramUserFile);
    }
    
    @Override
    public String authenticationMethod() {
        return ScramMechanism.SCRAM_SHA_256.getName();
    }

    @Override
    public ExtendedAuthenticator createAuthenticator() {
        return new ScramSha256Authenticator(scramUserLookup);
    }
}
