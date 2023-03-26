package io.moquette.persistence;

import io.moquette.broker.ISessionsRepository;
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
    private static final long UNDEFINED_INSTANT = -1;

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

        @Override
        public int getMemory(SessionData obj) {
            return stringDataType.getMemory(obj.clientId()) + 8 + 1 + 4;
        }

        @Override
        public void write(WriteBuffer buff, SessionData obj) {
            buff.put(SESSION_DATA_SERDES_V1);
            stringDataType.write(buff, obj.clientId());
            buff.putLong(obj.expiryInstant().orElse(UNDEFINED_INSTANT));
            buff.put(obj.protocolVersion().protocolLevel());
            buff.putInt(obj.expiryInterval());
        }

        @Override
        public SessionData read(ByteBuffer buff) {
            final byte serDesVersion = buff.get();
            if (serDesVersion != SESSION_DATA_SERDES_V1) {
                throw new IllegalArgumentException("Unrecognized serialization version " + serDesVersion);
            }
            final String clientId = stringDataType.read(buff);
            final long expiresAt = buff.getLong();
            final MqttVersion version = readMQTTVersion(buff.get());
            final int expiryInterval = buff.getInt();

            if (expiresAt == UNDEFINED_INSTANT) {
                return new SessionData(clientId, version, expiryInterval, clock);
            } else {
                return new SessionData(clientId, Instant.ofEpochMilli(expiresAt), version, expiryInterval, clock);
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
}
