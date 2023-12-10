/*
 * Copyright (c) 2012-2023 The original author or authors
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

import io.moquette.broker.subscriptions.Topic;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class DeclarativeAuthorizatorPolicy implements IAuthorizatorPolicy {
    public static final class Builder {
        private DeclarativeAuthorizatorPolicy instance;

        public Builder readFrom(Topic topic, String user, String client) {
            final DeclarativeAuthorizatorPolicy policy = createOrGet();
            policy.addReadFrom(topic, user, client);
            return this;
        }

        public Builder writeTo(Topic topic, String user, String client) {
            final DeclarativeAuthorizatorPolicy policy = createOrGet();
            policy.addWriteTo(topic, user, client);
            return this;
        }

        private DeclarativeAuthorizatorPolicy createOrGet() {
            if (instance == null) {
                instance = new DeclarativeAuthorizatorPolicy();
            }
            return instance;
        }

        public IAuthorizatorPolicy build() {
            return createOrGet();
        }
    }

    static final class TopicUserClient {
        final Topic topic;
        final String user;
        final String client;

        public TopicUserClient(Topic topic, String user, String client) {
            Objects.requireNonNull(topic);
            user = user == null ? "<>" : user;
            Objects.requireNonNull(client);
            this.topic = topic;
            this.user = user;
            this.client = client;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TopicUserClient that = (TopicUserClient) o;
            return Objects.equals(topic, that.topic) &&
                Objects.equals(user, that.user) &&
                Objects.equals(client, that.client);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, user, client);
        }
    }

    private final Set<TopicUserClient> readAdmitted = new HashSet<>();
    private final Set<TopicUserClient> writeAdmitted = new HashSet<>();

    protected void addWriteTo(Topic topic, String user, String client) {
        writeAdmitted.add(new TopicUserClient(topic, user, client));
    }

    protected void addReadFrom(Topic topic, String user, String client) {
        readAdmitted.add(new TopicUserClient(topic, user, client));
    }

    @Override
    public boolean canWrite(Topic topic, String user, String client) {
        return writeAdmitted.contains(new TopicUserClient(topic, user, client));
    }

    @Override
    public boolean canRead(Topic topic, String user, String client) {
        return readAdmitted.contains(new TopicUserClient(topic, user, client));
    }
}
