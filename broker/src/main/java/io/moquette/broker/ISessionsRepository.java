package io.moquette.broker;

import io.netty.handler.codec.mqtt.MqttVersion;

import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

/**
 * Used to store data about persisted sessions like MQTT version, session's properties.
 * */
public interface ISessionsRepository {

    // Data class
    final class SessionData {
        private final String clientId;
        private Instant expireAt = null;
        final MqttVersion version;
        private final int expiryInterval;

        /**
         * Construct a new SessionData without expiration set yet.
         * */
        public SessionData(String clientId, MqttVersion version, int expiryInterval) {
            this.clientId = clientId;
            this.version = version;
            this.expiryInterval = expiryInterval;
        }

        /**
         * Construct SessionData with an expiration instant, created by loading from the storage.
         * */
        public SessionData(String clientId, Instant expireAt, MqttVersion version, int expiryInterval) {
            this.expiryInterval = expiryInterval;
            Objects.requireNonNull(expireAt, "An expiration time is requested");
            this.clientId = clientId;
            this.expireAt = expireAt;
            this.version = version;
        }

        public String clientId() {
            return clientId;
        }

        public MqttVersion protocolVersion() {
            return version;
        }

        public Optional<Instant> expireAt() {
            return Optional.ofNullable(expireAt);
        }

        public Optional<Long> expiryInstant() {
            return expireAt()
                .map(Instant::toEpochMilli);
        }

        public int expiryInterval() {
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
                ", expireAt=" + expireAt +
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
