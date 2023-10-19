package io.moquette.persistence;

import io.moquette.broker.ISessionsRepository;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.BasicDataType;
import org.h2.mvstore.type.StringDataType;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.util.Collection;

class H2SessionsRepository implements ISessionsRepository {

    private static final byte SESSION_DATA_SERDES_V1 = 1;
    /* Contains also the Will message part */
    private static final byte SESSION_DATA_SERDES_V2 = 2;
    private static final long UNDEFINED_INSTANT = -1;
    public static final byte WILL_PRESENT = (byte) 0x01;
    public static final byte WILL_NOT_PRESENT = (byte) 0x00;

    private final MVMap<String, SessionData> sessionMap;
    private final Clock clock;

    public H2SessionsRepository(MVStore mvStore, Clock clock) {
        this.clock = clock;
        final MVMap.Builder<String, ISessionsRepository.SessionData> sessionTypeBuilder =
            new MVMap.Builder<String, ISessionsRepository.SessionData>()
                .valueType(new SessionDataValueType());

        this.sessionMap = mvStore.openMap("sessions_store", sessionTypeBuilder);
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

            return new Will(topic, payload, MqttQoS.valueOf(qos), retained);
        }

        @Override
        public Will[] createStorage(int i) {
            return new Will[i];
        }
    }
}
