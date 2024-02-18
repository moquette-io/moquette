package io.moquette.persistence;

import io.moquette.broker.IRetainedRepository;
import io.moquette.broker.RetainedMessage;
import io.moquette.broker.subscriptions.Topic;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class H2RetainedRepository implements IRetainedRepository {

    private final MVMap<Topic, RetainedMessage> retainedMap;
    private final MVMap<Topic, RetainedMessage> retainedExpireMap;

    public H2RetainedRepository(MVStore mvStore) {
        this.retainedMap = mvStore.openMap("retained_store");
        this.retainedExpireMap = mvStore.openMap("retained_expiry_store");
    }

    @Override
    public void cleanRetained(Topic topic) {
        retainedMap.remove(topic);
        retainedExpireMap.remove(topic);
    }

    @Override
    public void retain(Topic topic, MqttPublishMessage msg) {
        byte[] rawPayload = payloadToByteArray(msg);
        final RetainedMessage toStore = new RetainedMessage(topic, msg.fixedHeader().qosLevel(), rawPayload);
        retainedMap.put(topic, toStore);
    }

    @Override
    public void retain(Topic topic, MqttPublishMessage msg, Instant expiryTime) {
        byte[] rawPayload = payloadToByteArray(msg);
        final RetainedMessage toStore = new RetainedMessage(topic, msg.fixedHeader().qosLevel(), rawPayload, expiryTime);
        retainedExpireMap.put(topic, toStore);
    }

    private static byte[] payloadToByteArray(MqttPublishMessage msg) {
        final ByteBuf payload = msg.content();
        byte[] rawPayload = new byte[payload.readableBytes()];
        payload.getBytes(0, rawPayload);
        return rawPayload;
    }

    @Override
    public boolean isEmpty() {
        return retainedMap.isEmpty() && retainedExpireMap.isEmpty();
    }

    @Override
    public Collection<RetainedMessage> retainedOnTopic(String topic) {
        final Topic searchTopic = new Topic(topic);
        final List<RetainedMessage> matchingMessages = new ArrayList<>();
        matchingMessages.addAll(findMatching(searchTopic, retainedMap));
        matchingMessages.addAll(findMatching(searchTopic, retainedExpireMap));

        return matchingMessages;
    }

    private List<RetainedMessage> findMatching(Topic searchTopic, MVMap<Topic, RetainedMessage> mapToSearch) {
        final List<RetainedMessage> matchingMessages = new ArrayList<>();
        for (Map.Entry<Topic, RetainedMessage> entry : mapToSearch.entrySet()) {
            final Topic scanTopic = entry.getKey();
            if (scanTopic.match(searchTopic)) {
                matchingMessages.add(entry.getValue());
            }
        }
        return matchingMessages;
    }

    @Override
    public Collection<RetainedMessage> listExpirable() {
        return retainedExpireMap.values();
    }
}
