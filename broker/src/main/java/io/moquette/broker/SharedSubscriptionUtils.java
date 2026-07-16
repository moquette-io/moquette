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
package io.moquette.broker;

import java.util.Objects;

/**
 * Utility class that collects common utils methods for shared subscription topic parsing
 * */
class SharedSubscriptionUtils {

    /**
     * @return the share name in the topic filter of format $share/{shareName}/{topicFilter}
     * */
    // VisibleForTesting
    protected static String extractShareName(String sharedTopicFilter) {
        int afterShare = "$share/".length();
        int endOfShareName = sharedTopicFilter.indexOf('/', afterShare);
        return sharedTopicFilter.substring(afterShare, endOfShareName);
    }

    /**
     * @return the filter part from full topic filter of format $share/{shareName}/{topicFilter}
     * */
    // VisibleForTesting
    protected static String extractFilterFromShared(String fullSharedTopicFilter) {
        int afterShare = "$share/".length();
        int endOfShareName = fullSharedTopicFilter.indexOf('/', afterShare);
        return fullSharedTopicFilter.substring(endOfShareName + 1);
    }

    /**
     * @return true if topic filter is shared format
     * */
    protected static boolean isSharedSubscription(String topicFilter) {
        Objects.requireNonNull(topicFilter, "topicFilter can't be null");
        return topicFilter.startsWith("$share/");
    }

    /**
     * @return true if shareName is well-formed, is at least one characted and doesn't contain wildcard matchers
     * */
    protected static boolean validateShareName(String shareName) {
        // MQTT-4.8.2-1 MQTT-4.8.2-2, must be longer than 1 char and do not contain + or #
        Objects.requireNonNull(shareName);
        return shareName.length() > 0 && !shareName.contains("+") && !shareName.contains("#");
    }

    /**
     * Validate the WHOLE shared-subscription filter ($share/{shareName}/{topicFilter}) before it is
     * parsed. A filter such as "$share/grp" (a share name with no "/{topicFilter}" part) has no '/'
     * after the share name, which would make {@link #extractShareName(String)} evaluate
     * substring(7, -1) and throw. Returns true only when the share name and the topic-filter part are
     * both present and the share name is well-formed.
     *
     * @return true if topicFilter is a well-formed shared subscription
     * */
    protected static boolean isValidSharedSubscription(String topicFilter) {
        Objects.requireNonNull(topicFilter, "topicFilter can't be null");
        if (!isSharedSubscription(topicFilter)) {
            return false;
        }
        final int afterShare = "$share/".length();
        final int endOfShareName = topicFilter.indexOf('/', afterShare);
        if (endOfShareName < 0) {
            // no "/{topicFilter}" part after the share name
            return false;
        }
        if (!validateShareName(topicFilter.substring(afterShare, endOfShareName))) {
            return false;
        }
        // the topic-filter part must be present (non-empty)
        return endOfShareName + 1 < topicFilter.length();
    }
}
