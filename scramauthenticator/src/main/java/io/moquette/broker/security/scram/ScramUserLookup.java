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
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

/**
 * Finds the SCRAM data of a user in the users.yaml file, which is written by {@link ScramCredentialsGenerator}.
 * The file is loaded when the instance is created and then kept in memory by the instance, so a new instance
 * is needed to see users registered afterward.
 * */
public final class ScramUserLookup implements Function<String, ScramUserData> {

    private static final Logger LOG = LoggerFactory.getLogger(ScramUserLookup.class);

    /**
     * Result of the lookup for a user that is not registered (or when the users file is missing or unreadable).
     * It must be compared by identity, its fields don't hold usable credentials.
     * */
    public static final ScramUserData USER_NOT_PRESENT = new ScramUserData("", "", 0, "", "");

    private final Map<String, ScramUserData> users;

    /**
     * Loads all the user definitions at creation, the map is then shared by every caller of this instance.
     *
     * @param usersFile the users.yaml file.
     * */
    ScramUserLookup(Path usersFile) {
        this.users = loadUsers(usersFile);
    }

    private static Map<String, ScramUserData> loadUsers(Path usersFile) {
        try {
            Map<String, ScramUserData> loadedUsers = ScramCredentialsGenerator.loadUsers(usersFile);
            LOG.info("Load {} users from {}", loadedUsers.size(), usersFile);
            return Collections.unmodifiableMap(loadedUsers);
        } catch (IOException e) {
            LOG.warn("Cannot read the SCRAM users file {}, no user will be authenticated", usersFile, e);
            return Collections.emptyMap();
        }
    }

    @Override
    public ScramUserData apply(String username) {
        final ScramUserData data = users.get(username);
        LOG.info("Search username: {}, fetched: {}", username, data);
        return data != null ? data : USER_NOT_PRESENT;
    }
}
