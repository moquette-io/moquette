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
 * A TopicRewriter that does not rewrite.
 */
public class TopicRewriterUnity implements TopicRewriter {

    @Override
    public Topic rewriteTopic(Subscription subscription) {
        return subscription.getTopicFilterClient();
    }

    @Override
    public Topic rewriteTopicInverse(Topic clientTopic, Topic publishTopic) {
        return publishTopic;
    }

}
