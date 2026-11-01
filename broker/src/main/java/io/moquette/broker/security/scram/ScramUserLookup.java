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

import io.moquette.broker.security.scram.ScramCredentialsGenerator.ScramUserData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.Function;

/**
 * Finds the SCRAM data of a user in the users.yaml file, which is written by {@link ScramCredentialsGenerator}.
 * The file is loaded lazily at the first lookup and then kept in memory by this instance, so a new instance
 * is needed to see users registered afterwards.
 * */
public final class ScramUserLookup implements Function<String, ScramUserData> {

    private static final Logger LOG = LoggerFactory.getLogger(ScramUserLookup.class);

    /**
     * Result of the lookup for a user that is not registered (or when the users file is missing or unreadable).
     * It must be compared by identity, its fields don't hold usable credentials.
     * */
    public static final ScramUserData USER_NOT_PRESENT = new ScramUserData("", "", 0, "", "");

    private final Path usersFile;
    // loaded at the first lookup
    private volatile Map<String, ScramUserData> users;

    /**
     * @param usersFile the users.yaml file.
     * */
    public ScramUserLookup(Path usersFile) {
        this.usersFile = usersFile;
    }

    /**
     * Creates a lookup on the config/users.yaml file of the Moquette installation (system property moquette.path).
     * */
    public static ScramUserLookup forInstallation() {
        return new ScramUserLookup(Paths.get(System.getProperty("moquette.path", "."), "config", "users.yaml"));
    }

    @Override
    public ScramUserData apply(String username) {
        // TODO this should be thread safe!
        Map<String, ScramUserData> loaded = users;
        if (loaded == null) {
            try {
                loaded = ScramCredentialsGenerator.loadUsers(usersFile);
            } catch (IOException e) {
                // don't keep the failure, the next lookup tries to load again
                LOG.warn("Cannot read the SCRAM users file {}", usersFile, e);
                return USER_NOT_PRESENT;
            }
            users = loaded;
        }
        final ScramUserData data = loaded.get(username);
        return data != null ? data : USER_NOT_PRESENT;
    }
}
