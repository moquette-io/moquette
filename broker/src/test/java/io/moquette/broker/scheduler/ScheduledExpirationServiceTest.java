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

package io.moquette.broker.scheduler;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class ScheduledExpirationServiceTest {

    private static final class Entity implements Expirable {
        private final String id;
        private final Instant expireAt;

        private Entity(String id, Instant expireAt) {
            this.id = id;
            this.expireAt = expireAt;
        }

        @Override
        public Optional<Instant> expireAt() {
            return Optional.of(expireAt);
        }
    }

    @Test
    public void givenAnActionThatThrowsAnErrorWhenExecutedThenDoesntStopExecutingNextScheduledActions() {
        final Clock clock = Clock.fixed(Instant.ofEpochMilli(10_000), ZoneOffset.UTC);
        final Set<String> processed = new HashSet<>();
        final Consumer<Entity> action = e -> {
            if ("boom".equals(e.id)) {
                throw new RuntimeException("boom");
            }
            processed.add(e.id);
        };

        final ScheduledExpirationService<Entity> service = new ScheduledExpirationService<>(clock, action);
        try {
            final Instant alreadyExpired = Instant.ofEpochMilli(9_000);
            service.track("boom", new Entity("boom", alreadyExpired));
            service.track("good", new Entity("good", alreadyExpired));

            assertDoesNotThrow(service::checkExpiredEntities);
            assertThat(processed).contains("good");
        } finally {
            service.shutdown();
        }
    }
}
