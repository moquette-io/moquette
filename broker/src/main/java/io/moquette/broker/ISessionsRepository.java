package io.moquette.broker;

import io.moquette.broker.scheduler.Expirable;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * Used to store data about persisted sessions like MQTT version, session's properties.
 * */
public interface ISessionsRepository {

    // Data class
    final class SessionData implements Expirable {
        private final String clientId;
        private Instant expireAt = null;
        final MqttVersion version;

        final Optional<Will> will;
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
            this.will = Optional.empty();
        }

        public SessionData(String clientId, MqttVersion version, Will will, int expiryInterval, Clock clock) {
            this.clientId = clientId;
            this.clock = clock;
            this.expiryInterval = expiryInterval;
            this.version = version;
            this.will = Optional.of(will);
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
            this.will = Optional.empty();
        }

        public SessionData(String clientId, Instant expireAt, MqttVersion version, Will will, int expiryInterval, Clock clock) {
            Objects.requireNonNull(expireAt, "An expiration time is requested");
            this.clock = clock;
            this.clientId = clientId;
            this.expireAt = expireAt;
            this.expiryInterval = expiryInterval;
            this.version = version;
            this.will = Optional.of(will);
        }

        // Copy constructor
        private SessionData(String clientId, Instant expireAt, MqttVersion version, Optional<Will> will, int expiryInterval, Clock clock) {
            Objects.requireNonNull(expireAt, "An expiration time is requested");
            this.clock = clock;
            this.clientId = clientId;
            this.expireAt = expireAt;
            this.expiryInterval = expiryInterval;
            this.version = version;
            this.will = will;
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
            if (hasWill()) {
                return new SessionData(clientId, expiresAt, version, will, expiryInterval, clock);
            } else {
                return new SessionData(clientId, expiresAt, version, expiryInterval, clock);
            }
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

        public boolean hasWill() {
            return will.isPresent();
        }

        public Will will() throws IllegalArgumentException {
            return will.orElseThrow(() -> new IllegalArgumentException("Session's will is not available"));
        }

        public SessionData withWill(Will will) {
            if (expireAt != null) {
                return new SessionData(clientId, expireAt, version, will, expiryInterval, clock);
            } else {
                return new SessionData(clientId, version, will, expiryInterval, clock);
            }
        }

        public SessionData withoutWill() {
            if (expireAt != null) {
                return new SessionData(clientId, expireAt, version, expiryInterval, clock);
            } else {
                return new SessionData(clientId, version, expiryInterval, clock);
            }
        }
    }

    final class WillOptions {

        private final Duration messageExpiry;
        private final String contentType;
        private final String responseTopic;
        private final byte[] correlationData;
        private final Map<String, String> userProperties;
        private final boolean empty;

        private WillOptions(Duration messageExpiry, String contentType, String responseTopic, byte[] correlationData,
                            Map<String, String> userProperties, boolean empty) {
            this.messageExpiry = messageExpiry;
            this.contentType = contentType;
            this.responseTopic = responseTopic;
            this.correlationData = correlationData;
            this.userProperties = userProperties;
            this.empty = empty;
        }

        public static WillOptions empty() {
            return new WillOptions(null, null, null, null, null, true);
        }

        public WillOptions withMessageExpiry(Duration interval) {
            Objects.requireNonNull(interval, "A valid interval duration must be provided");
            return new WillOptions(interval, this.contentType, this.responseTopic, this.correlationData, this.userProperties, true);
        }

        public WillOptions withContentType(String contentType) {
            Objects.requireNonNull(contentType, "A valid contentType string must be provided");
            return new WillOptions(this.messageExpiry, contentType, this.responseTopic, this.correlationData, this.userProperties, true);
        }

        public WillOptions withResponseTopic(String responseTopic) {
            Objects.requireNonNull(responseTopic, "A valid responseTopic string must be provided");
            return new WillOptions(this.messageExpiry, this.contentType, responseTopic, this.correlationData, this.userProperties, true);
        }

        public WillOptions withCorrelationData(byte[] correlationData) {
            Objects.requireNonNull(correlationData, "A valid correlationData string must be provided");
            return new WillOptions(this.messageExpiry, this.contentType, this.responseTopic, correlationData, this.userProperties, true);
        }

        public WillOptions withUserProperties(Map<String, String> properties) {
            Objects.requireNonNull(properties, "A valid properties Map must be provided");
            return new WillOptions(this.messageExpiry, this.contentType, this.responseTopic, this.correlationData, properties, true);
        }

        public Optional<Duration> messageExpiry() {
            return Optional.ofNullable(this.messageExpiry);
        }

        public Optional<String> contentType() {
            return Optional.ofNullable(this.contentType);
        }

        public Optional<String> responseTopic() {
            return Optional.ofNullable(this.responseTopic);
        }

        public Optional<byte[]> correlationData() {
            return Optional.ofNullable(this.correlationData);
        }

        public Optional<Map<String, String>> userProperties() {
            return Optional.ofNullable(this.userProperties);
        }

        public boolean notEmpty() {
            return empty;
        }
    }

     final class Will implements Expirable {

         public final String topic;
         public final byte[] payload;
         public final MqttQoS qos;
         public final boolean retained;
         public final int delayInterval;
         public final WillOptions properties;
         private Instant expireAt = null;

         public Will(String topic, byte[] payload, MqttQoS qos, boolean retained, int delayInterval) {
             this.topic = topic;
             this.payload = payload;
             this.qos = qos;
             this.retained = retained;
             this.delayInterval = delayInterval;
             this.properties = ISessionsRepository.WillOptions.empty();
         }

         /**
          * Copy constructor used only when update with an expire instant.
          * */
         public Will(Will orig, Instant expireAt) {
            this(orig.topic, orig.payload, orig.qos, orig.retained, orig.delayInterval, orig.properties);
            this.expireAt = expireAt;
         }

         /**
          * Used only when update with optional properties are provided.
          * */
         public Will(Will orig, WillOptions properties) {
             this.topic = orig.topic;
             this.payload = orig.payload;
             this.qos = orig.qos;
             this.retained = orig.retained;
             this.delayInterval = orig.delayInterval;
             this.expireAt = orig.expireAt;
             this.properties = properties;
         }

         private Will(String topic, byte[] payload, MqttQoS qos, boolean retained, int delayInterval,
                      WillOptions properties) {
             this.topic = topic;
             this.payload = payload;
             this.qos = qos;
             this.retained = retained;
             this.delayInterval = delayInterval;
             this.properties = properties;
         }

         @Override
         public Optional<Instant> expireAt() {
             return Optional.ofNullable(expireAt);
         }

         public Will withExpirationComputed(int executionInterval, Clock clock) {
             return new Will(this, clock.instant().plusSeconds(executionInterval));
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

    /**
     * Return the will definitions.
     * Invokes a callback function accepting clientId and Will
     * */
    void listSessionsWill(BiConsumer<String, Will> visitor);

    /**
     * Persist the Will specification for client.
     *
     * @param clientId Identifier of the client that the Will belongs to.
     * @param will Will specification to store.
     * */
    void saveWill(String clientId, ISessionsRepository.Will will);

    /**
     * Delete the Will specification of a client.
     *
     * @param clientId the identifier of the client.
     * */
    void deleteWill(String clientId);
}
