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
}
