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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class ScramUserLookupTest {

    @Test
    void findsRegisteredUser(@TempDir Path dir) throws IOException {
        final Path file = dir.resolve("config").resolve("users.yaml");
        final ScramUserData bob = ScramCredentialsGenerator.registerNewUser("bob", "pw".toCharArray(), 4096);
        ScramCredentialsGenerator.storeUser(file, bob);

        final ScramUserData found = new ScramUserLookup(file).apply("bob");
        assertEquals("bob", found.getUsername());
        assertEquals(bob.getBase64Salt(), found.getBase64Salt());
        assertEquals(bob.getBase64StoredKey(), found.getBase64StoredKey());
        assertEquals(bob.getBase64ServerKey(), found.getBase64ServerKey());
        assertEquals(4096, found.getIterationCount());
    }

    @Test
    void unknownUser(@TempDir Path dir) throws IOException {
        final Path file = dir.resolve("users.yaml");
        ScramCredentialsGenerator.storeUser(file, ScramCredentialsGenerator.registerNewUser("bob", "pw".toCharArray(), 4096));

        assertSame(ScramUserLookup.USER_NOT_PRESENT, new ScramUserLookup(file).apply("alice"));
    }

    @Test
    void missingFile(@TempDir Path dir) {
        assertSame(ScramUserLookup.USER_NOT_PRESENT, new ScramUserLookup(dir.resolve("users.yaml")).apply("bob"));
    }

    @Test
    void unreadableFileIsTreatedAsNotPresent(@TempDir Path dir) throws IOException {
        final Path file = dir.resolve("users.yaml");
        Files.write(file, "not a users file".getBytes(StandardCharsets.UTF_8));

        assertSame(ScramUserLookup.USER_NOT_PRESENT, new ScramUserLookup(file).apply("bob"));
    }

    @Test
    void usersAreLoadedOnceAtFirstLookup(@TempDir Path dir) throws IOException {
        final Path file = dir.resolve("users.yaml");
        ScramCredentialsGenerator.storeUser(file, ScramCredentialsGenerator.registerNewUser("bob", "pw".toCharArray(), 4096));
        final ScramUserLookup lookup = new ScramUserLookup(file);
        assertEquals("bob", lookup.apply("bob").getUsername());

        // registered after the first lookup, not seen by the same instance
        ScramCredentialsGenerator.storeUser(file, ScramCredentialsGenerator.registerNewUser("alice", "pw".toCharArray(), 4096));
        assertSame(ScramUserLookup.USER_NOT_PRESENT, lookup.apply("alice"));
        assertEquals("alice", new ScramUserLookup(file).apply("alice").getUsername());
    }

    @Test
    void failedLoadIsRetriedAtNextLookup(@TempDir Path dir) throws IOException {
        final Path file = dir.resolve("users.yaml");
        Files.write(file, "not a users file".getBytes(StandardCharsets.UTF_8));
        final ScramUserLookup lookup = new ScramUserLookup(file);
        assertSame(ScramUserLookup.USER_NOT_PRESENT, lookup.apply("bob"));

        Files.delete(file);
        ScramCredentialsGenerator.storeUser(file, ScramCredentialsGenerator.registerNewUser("bob", "pw".toCharArray(), 4096));
        assertEquals("bob", lookup.apply("bob").getUsername());
    }
}
