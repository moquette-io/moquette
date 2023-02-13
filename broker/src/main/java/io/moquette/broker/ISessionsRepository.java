package io.moquette.broker;

import io.netty.handler.codec.mqtt.MqttVersion;

import java.time.Instant;
import java.util.Collection;
import java.util.Objects;

/**
 * Used to store data about persisted sessions like MQTT version, session's properties.
 * */
public interface ISessionsRepository {

    // Data class
    final class SessionData {
        private final String clientId;
        private final Instant created;
        final MqttVersion version;
        final long expiryInterval;

        public SessionData(String clientId, Instant created, MqttVersion version, long expiryInterval) {
            this.clientId = clientId;
            this.created = created;
            this.version = version;
            this.expiryInterval = expiryInterval;
        }

        public String clientId() {
            return clientId;
        }

        public MqttVersion protocolVersion() {
            return version;
        }

        public Instant created() {
            return created;
        }

        public long expiryInterval() {
            return expiryInterval;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SessionData that = (SessionData) o;
            return clientId.equals(that.clientId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clientId);
        }

        @Override
        public String toString() {
            return "SessionData{" +
                "clientId='" + clientId + '\'' +
                ", created=" + created +
                ", version=" + version +
                ", expiryInterval=" + expiryInterval +
                '}';
        }
    }

    /**
     * @return the full list of persisted sessions data.
     * */
    Collection<SessionData> list();

    /**
     * Save data composing a session, es MQTT version, creation date and properties but not queues or subscriptions.
     * */
    void saveSession(SessionData session);
}
