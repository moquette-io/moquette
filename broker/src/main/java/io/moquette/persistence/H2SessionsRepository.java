package io.moquette.persistence;

import io.moquette.broker.ISessionsRepository;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.BasicDataType;
import org.h2.mvstore.type.ByteArrayDataType;
import org.h2.mvstore.type.StringDataType;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

class H2SessionsRepository implements ISessionsRepository {

    private static final byte SESSION_DATA_SERDES_V1 = 1;
    /* Contains also the Will message part */
    private static final byte SESSION_DATA_SERDES_V2 = 2;
    private static final long UNDEFINED_INSTANT = -1;
    public static final byte WILL_PRESENT = (byte) 0x01;
    public static final byte WILL_NOT_PRESENT = (byte) 0x00;

    private final MVMap<String, SessionData> sessionMap;
    private final MVMap<String, Will> willMap;
    private final Clock clock;

    public H2SessionsRepository(MVStore mvStore, Clock clock) {
        this.clock = clock;
        final MVMap.Builder<String, ISessionsRepository.SessionData> sessionTypeBuilder =
            new MVMap.Builder<String, ISessionsRepository.SessionData>()
                .valueType(new SessionDataValueType());

        this.sessionMap = mvStore.openMap("sessions_store", sessionTypeBuilder);

        MVMap.Builder<String, Will> willTypeBuilder = new MVMap.Builder<String, Will>()
            .valueType(new WillDataValueType());
        this.willMap = mvStore.openMap("will_specs_store", willTypeBuilder);
    }

    @Override
    public Collection<SessionData> list() {
        return sessionMap.values();
    }

    @Override
    public void saveSession(SessionData session) {
        sessionMap.put(session.clientId(), session);
    }

    @Override
    public void delete(SessionData session) {
        sessionMap.remove(session.clientId());
    }

    @Override
    public void listSessionsWill(BiConsumer<String, Will> visitor) {
        willMap.entrySet().stream()
            .forEach(e -> visitor.accept(e.getKey(), e.getValue()));
    }

    @Override
    public void saveWill(String clientId, Will will) {
        willMap.put(clientId, will);
    }

    @Override
    public void deleteWill(String clientId) {
        willMap.remove(clientId);
    }

    /**
     * Codec data type to load and store SessionData instances
     */
    private final class SessionDataValueType extends BasicDataType<SessionData> {

        private final StringDataType stringDataType = new StringDataType();
        private final WillDataValueType willDataType = new WillDataValueType();

        @Override
        public int getMemory(SessionData obj) {
            int length = stringDataType.getMemory(obj.clientId()) + 8 + 1 + 4 +
                1 /* flag to indicate if will is present or not */;
            if (obj.hasWill()) {
                return length + willDataType.getMemory(obj.will());
            } else {
                return length;
            }
        }

        @Override
        public void write(WriteBuffer buff, SessionData obj) {
            buff.put(SESSION_DATA_SERDES_V2);
            stringDataType.write(buff, obj.clientId());
            buff.putLong(obj.expiryInstant().orElse(UNDEFINED_INSTANT));
            buff.put(obj.protocolVersion().protocolLevel());
            buff.putInt(obj.expiryInterval());
            if (obj.hasWill()) {
                buff.put(WILL_PRESENT);
                willDataType.write(buff, obj.will());
            } else {
                buff.put(WILL_NOT_PRESENT);
            }
        }

        @Override
        public SessionData read(ByteBuffer buff) {
            final byte serDesVersion = buff.get();
            if (serDesVersion != SESSION_DATA_SERDES_V1 && serDesVersion != SESSION_DATA_SERDES_V2) {
                throw new IllegalArgumentException("Unrecognized serialization version " + serDesVersion);
            }
            final String clientId = stringDataType.read(buff);
            final long expiresAt = buff.getLong();
            final MqttVersion version = readMQTTVersion(buff.get());
            final int expiryInterval = buff.getInt();

            Will will = null;
            if (serDesVersion == SESSION_DATA_SERDES_V2 && buff.get() == WILL_PRESENT) {
                will = willDataType.read(buff);
            }

            if (expiresAt == UNDEFINED_INSTANT) {
                if (will != null) {
                    return new SessionData(clientId, version, will, expiryInterval, clock);
                } else {
                    return new SessionData(clientId, version, expiryInterval, clock);
                }
            } else {
                if (will != null) {
                    return new SessionData(clientId, Instant.ofEpochMilli(expiresAt), version, will, expiryInterval, clock);
                } else {
                    return new SessionData(clientId, Instant.ofEpochMilli(expiresAt), version, expiryInterval, clock);
                }
            }
        }

        @Override
        public SessionData[] createStorage(int i) {
            return new SessionData[i];
        }
    }

    private MqttVersion readMQTTVersion(byte rawVersion) {
        final MqttVersion version;
        switch (rawVersion) {
            case 3: version = MqttVersion.MQTT_3_1; break;
            case 4: version = MqttVersion.MQTT_3_1_1; break;
            case 5: version = MqttVersion.MQTT_5; break;
            default:
                throw new IllegalArgumentException("Unrecognized MQTT version value " + rawVersion);
        }
        return version;
    }

    /**
     * Codec data type to load and store message's Will part.
     * */
    private final class WillDataValueType extends BasicDataType<Will> {

        private final StringDataType stringDataType = new StringDataType();
        private final WillOptionsDataValueType willOptionsDataType = new WillOptionsDataValueType();

        @Override
        public int getMemory(Will will) {
            return stringDataType.getMemory(will.topic) +
                4 + /* payload length */
                will.payload.length +
                1 /* retained + qos */;
        }

        @Override
        public void write(WriteBuffer buff, Will will) {
            stringDataType.write(buff, will.topic);
            buff.putInt(will.payload.length);
            buff.put(will.payload);

            // MSB retained, LSB QoS
            byte retained = will.retained ? (byte) 0x10 : 0x00;
            byte qos = (byte) (will.qos.value() & 0x0F);
            buff.put((byte) (retained & qos));
            buff.putInt(will.delayInterval);
            buff.putLong(will.expireAt().map(Instant::toEpochMilli).orElse(UNDEFINED_INSTANT));
            if (will.properties.notEmpty()) {
                willOptionsDataType.write(buff, will.properties);
            }
        }

        @Override
        public Will read(ByteBuffer buff) {
            final String topic = stringDataType.read(buff);
            final int payloadLength = buff.getInt();
            final byte[] payload = new byte[payloadLength];
            buff.get(payload);
            final byte rawFlags = buff.get();
            // MSB retained, LSB QoS
            final byte qos = (byte) (rawFlags & 0x0F);
            final boolean retained = ((rawFlags >> 4) & 0x0F) > 0;
            final int willDelayInterval = buff.getInt();
            Will will = new Will(topic, payload, MqttQoS.valueOf(qos), retained, willDelayInterval);

            final long expiresAt = buff.getLong();
            if (expiresAt != UNDEFINED_INSTANT) {
                will = new Will(will, Instant.ofEpochMilli(expiresAt));
            }

            final WillOptions options = willOptionsDataType.read(buff);
            if (options != null && options.notEmpty()) {
                will = new Will(will, options);
            }

            return will;
        }

        @Override
        public Will[] createStorage(int i) {
            return new Will[i];
        }
    }

    private static final class WillOptionsDataValueType extends BasicDataType<WillOptions> {

        private static final byte EMPTY_OPTIONS = -1;
        private static final byte MESSAGE_EXPIRY_FLAG = 0x01;
        private static final byte CONTENT_TYPE_FLAG = MESSAGE_EXPIRY_FLAG << 1;
        private static final byte RESPONSE_TOPIC_FLAG = CONTENT_TYPE_FLAG << 1;
        private static final byte CORRELATION_DATA_FLAG = RESPONSE_TOPIC_FLAG << 1;
        private static final byte USER_PROPERTIES_FLAG = CORRELATION_DATA_FLAG << 1;
        private final DurationDataValueType durationDataValueType = new DurationDataValueType();
        private final StringDataType stringDataType = StringDataType.INSTANCE;
        private final ByteArrayDataType byteArrayDataType = ByteArrayDataType.INSTANCE;
        private final UserPropertiesDataValueType userPropertiesDataValueType = new UserPropertiesDataValueType();

        @Override
        public int getMemory(WillOptions willOptions) {
            int size = 1; //the header with option bitmask

            if (willOptions.messageExpiry().isPresent()) {
                size += durationDataValueType.getMemory(willOptions.messageExpiry().get());
            }
            if (willOptions.contentType().isPresent()) {
                size += stringDataType.getMemory(willOptions.contentType().get());
            }
            if (willOptions.responseTopic().isPresent()) {
                size += stringDataType.getMemory(willOptions.responseTopic().get());
            }
            if (willOptions.correlationData().isPresent()) {
                size += byteArrayDataType.getMemory(willOptions.correlationData().get());
            }
            if (willOptions.userProperties().isPresent()) {
                size += userPropertiesDataValueType.getMemory(willOptions.userProperties().get());
            }
            return size;
        }

        @Override
        public void write(WriteBuffer writeBuffer, WillOptions willOptions) {
            if (willOptions == null || !willOptions.notEmpty()) {
                writeBuffer.put(EMPTY_OPTIONS);
                return;
            }

            // determine which options are present and compute the bitmask
            byte optionBitmask = 0x00;
            if (willOptions.messageExpiry().isPresent()) {
                optionBitmask = (byte) (optionBitmask | MESSAGE_EXPIRY_FLAG);
            }
            if (willOptions.contentType().isPresent()) {
                optionBitmask = (byte) (optionBitmask | CONTENT_TYPE_FLAG);
            }
            if (willOptions.responseTopic().isPresent()) {
                optionBitmask = (byte) (optionBitmask | RESPONSE_TOPIC_FLAG);
            }
            if (willOptions.correlationData().isPresent()) {
                optionBitmask = (byte) (optionBitmask | CORRELATION_DATA_FLAG);
            }
            if (willOptions.userProperties().isPresent()) {
                optionBitmask = (byte) (optionBitmask | USER_PROPERTIES_FLAG);
            }
            writeBuffer.put(optionBitmask);

            willOptions.messageExpiry().ifPresent(expiry -> durationDataValueType.write(writeBuffer, expiry));
            willOptions.contentType().ifPresent(contentType -> stringDataType.write(writeBuffer, contentType));
            willOptions.responseTopic().ifPresent(responseTopic -> stringDataType.write(writeBuffer, responseTopic));
            willOptions.correlationData().ifPresent(correlationData -> byteArrayDataType.write(writeBuffer, correlationData));
            willOptions.userProperties().ifPresent(userProps -> userPropertiesDataValueType.write(writeBuffer, userProps));
        }

        @Override
        public WillOptions read(ByteBuffer byteBuffer) {
            byte header = byteBuffer.get();
            WillOptions options = WillOptions.empty();
            if (header == EMPTY_OPTIONS) {
                return options;
            }
            if ((header & MESSAGE_EXPIRY_FLAG) > 0) {
                options = options.withMessageExpiry(durationDataValueType.read(byteBuffer));
            }
            if ((header & CONTENT_TYPE_FLAG) > 0) {
                options = options.withContentType(stringDataType.read(byteBuffer));
            }
            if ((header & RESPONSE_TOPIC_FLAG) > 0) {
                options = options.withResponseTopic(stringDataType.read(byteBuffer));
            }
            if ((header & CORRELATION_DATA_FLAG) > 0) {
                options = options.withCorrelationData(byteArrayDataType.read(byteBuffer));
            }
            if ((header & USER_PROPERTIES_FLAG) > 0) {
                options = options.withUserProperties(userPropertiesDataValueType.read(byteBuffer));
            }
            return options;
        }

        @Override
        public WillOptions[] createStorage(int i) {
            return new WillOptions[i];
        }
    }

    private static final class DurationDataValueType extends BasicDataType<Duration> {

        @Override
        public int getMemory(Duration duration) {
            return 4;
        }

        @Override
        public void write(WriteBuffer writeBuffer, Duration duration) {
            int seconds = (int) duration.get(ChronoUnit.SECONDS);
            writeBuffer.putInt(seconds);
        }

        @Override
        public Duration read(ByteBuffer byteBuffer) {
            return Duration.ofSeconds(byteBuffer.getInt());
        }

        @Override
        public Duration[] createStorage(int i) {
            return new Duration[i];
        }
    }

    private static final class UserPropertiesDataValueType extends BasicDataType<Map<String, String>> {

        @Override
        public int getMemory(Map<String, String> userProps) {
            int kvSize = userProps.entrySet().stream()
                .map(kv -> StringDataType.INSTANCE.getMemory(kv.getKey()) + StringDataType.INSTANCE.getMemory(kv.getValue()))
                .mapToInt(Integer::intValue)
                .sum();
            return 4 + // length of the key, value couples list
                   kvSize;
        }

        @Override
        public void write(WriteBuffer writeBuffer, Map<String, String> userProps) {
            writeBuffer.putInt(userProps.size());
            for (Map.Entry<String, String> kv : userProps.entrySet()) {
                StringDataType.INSTANCE.write(writeBuffer, kv.getKey());
                StringDataType.INSTANCE.write(writeBuffer, kv.getValue());
            }
        }

        @Override
        public Map<String, String> read(ByteBuffer byteBuffer) {
            int length = byteBuffer.getInt();
            final Map<String, String> result = new HashMap(length);
            for (int i = 0; i < length; i++) {
                String key = StringDataType.INSTANCE.read(byteBuffer);
                String value = StringDataType.INSTANCE.read(byteBuffer);
                result.put(key, value);
            }
            return result;
        }

        @Override
        public Map<String, String>[] createStorage(int i) {
            return new Map[i];
        }
    }
}
