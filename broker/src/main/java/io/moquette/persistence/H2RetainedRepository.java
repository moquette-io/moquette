package io.moquette.persistence;

import io.moquette.broker.IRetainedRepository;
import io.moquette.broker.RetainedMessage;
import io.moquette.broker.subscriptions.Subscription;
import io.moquette.broker.subscriptions.Topic;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.BasicDataType;
import org.h2.mvstore.type.StringDataType;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class H2RetainedRepository implements IRetainedRepository {

    private final MVMap<Topic, RetainedMessage> retainedMap;
    private final MVMap<Topic, RetainedMessage> retainedExpireMap;

    private final MVMap.Builder<Topic, RetainedMessage> retainedBuilder = new MVMap.Builder<Topic, RetainedMessage>()
        .valueType(new RetainedMessageValueType());

    public H2RetainedRepository(MVStore mvStore) {
//        this.retainedMap = mvStore.openMap("retained_store");
        this.retainedMap = mvStore.openMap("retained_store", retainedBuilder);
//        this.retainedExpireMap = mvStore.openMap("retained_expiry_store");
        this.retainedExpireMap = mvStore.openMap("retained_expiry_store", retainedBuilder);
    }

    @Override
    public void cleanRetained(Topic topic) {
        retainedMap.remove(topic);
        retainedExpireMap.remove(topic);
    }

    @Override
    public void retain(Topic topic, MqttPublishMessage msg) {
        byte[] rawPayload = payloadToByteArray(msg);
        final RetainedMessage toStore = new RetainedMessage(topic, msg.fixedHeader().qosLevel(), rawPayload, extractPropertiesArray(msg));
        retainedMap.put(topic, toStore);
    }

    private static MqttProperties.MqttProperty[] extractPropertiesArray(MqttPublishMessage msg) {
        MqttProperties properties = msg.variableHeader().properties();
        return properties.listAll().toArray(new MqttProperties.MqttProperty[0]);
    }

    @Override
    public void retain(Topic topic, MqttPublishMessage msg, Instant expiryTime) {
        byte[] rawPayload = payloadToByteArray(msg);
        final RetainedMessage toStore = new RetainedMessage(topic, msg.fixedHeader().qosLevel(), rawPayload, extractPropertiesArray(msg), expiryTime);
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

    private static final class RetainedMessageValueType extends BasicDataType<RetainedMessage> {
        // Layout for RetainedMessage:
        // - topic String
        // - qos int
        // - payload byte[]
        // - flag map to say if contains properties, expiry time (MSB, LSB)
        // - (opt) expiry time in epoch millis long
        // - (opt) properties

        private final PropertiesDataType propertiesDataType = new PropertiesDataType();

        private static final byte MESSAGE_EXPIRY_FLAG = 0x01;
        private static final byte PROPERTIES_FLAG = MESSAGE_EXPIRY_FLAG << 1;

        @Override
        public int getMemory(RetainedMessage retainedMsg) {
            int bytesSize = StringDataType.INSTANCE.getMemory(retainedMsg.getTopic().toString()) +
                1 + // qos, 1 byte
                4 + retainedMsg.getPayload().length + // length + bytes
                1; // flags
            if (retainedMsg.getExpiryTime() != null) {
                bytesSize += 8; // long
            }
            int propertiesSize = retainedMsg.getMqttProperties().length > 0 ?
                propertiesDataType.getMemory(retainedMsg.getMqttProperties()) :
                0;

            return bytesSize + propertiesSize;
        }

        @Override
        public void write(WriteBuffer buff, RetainedMessage retainedMsg) {
            StringDataType.INSTANCE.write(buff, retainedMsg.getTopic().toString());
            buff.put((byte) retainedMsg.qosLevel().value());
            buff.putInt(retainedMsg.getPayload().length);
            buff.put(retainedMsg.getPayload());

            byte flagsBitmask = 0x00;
            if (retainedMsg.getExpiryTime() != null) {
                flagsBitmask = (byte) (flagsBitmask | MESSAGE_EXPIRY_FLAG);
            }
            if (retainedMsg.getMqttProperties().length > 0) {
                flagsBitmask = (byte) (flagsBitmask | PROPERTIES_FLAG);
            }

            buff.put(flagsBitmask);

            if (retainedMsg.getExpiryTime() != null) {
                buff.putLong(retainedMsg.getExpiryTime().toEpochMilli());
            }

            if (retainedMsg.getMqttProperties().length > 0) {
                propertiesDataType.write(buff, retainedMsg.getMqttProperties());
            }
        }

        @Override
        public RetainedMessage read(ByteBuffer buff) {
            final String topicStr = StringDataType.INSTANCE.read(buff);
            final MqttQoS qos = MqttQoS.valueOf(buff.get());

            final int payloadSize = buff.getInt();
            byte[] payload = new byte[payloadSize];
            buff.get(payload);

            final byte flags = buff.get();

            final Instant expiry;
            if ((flags & MESSAGE_EXPIRY_FLAG) > 0) {
                long millis = buff.getLong();
                expiry = Instant.ofEpochMilli(millis);
            } else {
                expiry = null;
            }

            final MqttProperties.MqttProperty[] mqttProperties;
            if ((flags & PROPERTIES_FLAG) > 0) {
                mqttProperties = propertiesDataType.read(buff);
            } else {
                mqttProperties = new MqttProperties.MqttProperty[0];
            }

            if ((flags & MESSAGE_EXPIRY_FLAG) > 0) {
                return new RetainedMessage(new Topic(topicStr), qos, payload, mqttProperties, expiry);
            } else {
                return new RetainedMessage(new Topic(topicStr), qos, payload, mqttProperties);
            }
        }

        @Override
        public RetainedMessage[] createStorage(int size) {
            return new RetainedMessage[size];
        }
    }
}
