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

package io.moquette.broker;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedSubscriptionUtilsTest {

    @Test
    public void givenWellFormedSharedSubscriptionWhenValidatedThenGetPositiveAnswer() {
        assertTrue(SharedSubscriptionUtils.isValidSharedSubscription("$share/grp/sensors/#"));
        assertTrue(SharedSubscriptionUtils.isValidSharedSubscription("$share/grp/a/b"));
        assertTrue(SharedSubscriptionUtils.isValidSharedSubscription("$share/grp/+"));
    }

    @Test
    public void givenSharedSubscriptionWithNoTopicFilterPartWhenValidatedThenIsRejected() {
        // "$share/grp" has no "/{topicFilter}" part: extractShareName() would have evaluated
        // substring(7, -1) and thrown StringIndexOutOfBoundsException while building the subscription.
        assertFalse(SharedSubscriptionUtils.isValidSharedSubscription("$share/grp"));
        assertFalse(SharedSubscriptionUtils.isValidSharedSubscription("$share/grp/"));
    }

    @Test
    public void givenSharedSubscriptionWithEmptyOrWildcardShareNameWhenValidatedThenIsRejected() {
        assertFalse(SharedSubscriptionUtils.isValidSharedSubscription("$share//sensors"));
        assertFalse(SharedSubscriptionUtils.isValidSharedSubscription("$share/gr+p/sensors"));
        assertFalse(SharedSubscriptionUtils.isValidSharedSubscription("$share/gr#p/sensors"));
    }

    @Test
    public void givenNonSharedFilterWhenValidatedThenIsNotAValidSharedSubscription() {
        assertFalse(SharedSubscriptionUtils.isValidSharedSubscription("sensors/#"));
    }
}
