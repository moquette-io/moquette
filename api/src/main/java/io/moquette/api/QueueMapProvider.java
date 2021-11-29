package io.moquette.api;

import java.util.Map;
import java.util.Set;

public interface QueueMapProvider<M extends Map<?,?>, V> {
    String QUEUE_PREFIX = "queue_";
    String QUEUE_META_SUFFIX = "_meta";

    Set<String> getMapNames();

    Map<Long, V> queueMap(String queueName);

    Map<String, Long> metadataMap(String queueName);

    void removeMap(M mapToRemove);

    static String queueMapName(String queueName) {
        return QUEUE_PREFIX + queueName;
    }

    static String queueMapMetadataName(String queueName) {
        return queueMapName(queueName) + QUEUE_META_SUFFIX;
    }
}
