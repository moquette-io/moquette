/*
 * Copyright (c) 2012-2025 The original author or authors
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
package io.moquette.interception;

import io.moquette.broker.subscriptions.Subscription;
import io.moquette.broker.subscriptions.Topic;

/**
 * A topic re-writer can change the topics of subscriptions and publishes before
 * they are handled internally.
 *
 * <h2>Topic filter without wildcards</h2>
 *
 *  <p>Subscribes subscribes to {@code old/sensor1}, which the rewriter rewrites
 * to {@code new/sensor1} using the method {@link #rewriteTopic(Subscription)}.
 * This means that on the subscriptions tree we register the subscription under
 * the {@code new/sensor1} path. When a publish matching {@code new/sensor1} is
 * received a new publish message is emitted with a topic name of the client, so
 * {@code old/sensor1}.</p>
 *
 *  <h2>Topic filter with wildcards</h2>
 *
 *  <p>Subscribes subscribes to {@code old/#}, which the rewriter rewrites to
 * {@code new/#}.
 * This means that on the subscriptions tree we register the subscription under
 * the {@code new/#} path. When a publish matching {@code new/#} is received, for
 * example {@code new/sensor1}, a new publish message is emitted with a topic
 * name as it matched the client's topic filter, so {@code old/sensor1}. In this
 * case we can't user {@code topicFilterClient} because it would be {@code old/#}
 * and we need the {@link #rewriteTopicInverse(Topic, Topic)} method to convert
 * {@code new/sensor1} to {@code old/sensor1}.</p>
 */
public interface TopicRewriter {

    /**
     * Rewrite the topic for the given subscription.
     *
     * @param subscription The subscription to rewrite the Topic for.
     * @return the rewritten topic.
     */
    Topic rewriteTopic(Subscription subscription);

    /**
     * Reverse the rewrite for the given publish Topic, so that it matches the
     * clientTopic. This is needed when a subscription that this topic matched
     * (the clientTopic) has a wild-card and is rewritten.
     *
     * @param clientTopic The topic that the client originally subscribed to.
     * This will have a wild-card in it.
     * @param publishedTopic The topic that received a publish.
     * @return The topic that the client would expect the publish to be on.
     */
    Topic rewriteTopicInverse(Topic clientTopic, Topic publishedTopic);
}
