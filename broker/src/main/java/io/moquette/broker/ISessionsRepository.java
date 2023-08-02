package io.moquette.broker;

import io.netty.handler.codec.mqtt.MqttVersion;

import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Used to store data about persisted sessions like MQTT version, session's properties.
 * */
public interface ISessionsRepository {

    // Data class
    final class SessionData implements Delayed {
        private final String clientId;
        private Instant expireAt = null;
        final MqttVersion version;
        private final int expiryInterval;
        private transient final Clock clock;

        /**
         * Construct a new SessionData without expiration set yet.
         *
         * @param expiryInterval seconds after which the persistent session could be dropped.
         * */
        public SessionData(String clientId, MqttVersion version, int expiryInterval, Clock clock) {
            this.clientId = clientId;
            this.clock = clock;
            this.expiryInterval = expiryInterval;
            this.version = version;
        }

        /**
         * Construct SessionData with an expiration instant, created by loading from the storage.
         *
         * @param expiryInterval seconds after which the persistent session could be dropped.
         * */
        public SessionData(String clientId, Instant expireAt, MqttVersion version, int expiryInterval, Clock clock) {
            Objects.requireNonNull(expireAt, "An expiration time is requested");
            this.clock = clock;
            this.clientId = clientId;
            this.expireAt = expireAt;
            this.expiryInterval = expiryInterval;
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

        public SessionData withExpirationComputed() {
            final Instant expiresAt = clock.instant().plusSeconds(expiryInterval);
            return new SessionData(clientId, expiresAt, version, expiryInterval, clock);
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

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(expireAt.toEpochMilli() - clock.millis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
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

    void delete(SessionData session);
}
