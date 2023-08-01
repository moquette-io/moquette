package io.moquette.broker;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.awaitility.Awaitility;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.assertEquals;
public class SessionRegistryMQTT5Test extends SessionRegistryTest {
    private static final Logger LOG = LoggerFactory.getLogger(SessionRegistryMQTT5Test.class);
    @Test
    public void givenSessionWithConnectionExpireTimeWhenAfterExpirationIsPassedThenSessionIsRemoved() {
        LOG.info("givenSessionWithExpireTimeWhenAfterExpirationIsPassedThenSessionIsRemoved");
        // insert a not clean session that should expire in connect selected expiry time
        final String clientId = "client_to_be_removed";
        final MqttProperties connectProperties = new MqttProperties();
        int customExpirySeconds = 60;
        connectProperties.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value(), customExpirySeconds));
        final MqttConnectMessage connectMessage = MqttMessageBuilders.connect()
            .protocolVersion(MqttVersion.MQTT_5)
            .cleanSession(false)
            .properties(connectProperties)
            .build();
        final SessionRegistry.SessionCreationResult res = sut.createOrReopenSession(connectMessage, clientId, "User");
        assertEquals(SessionRegistry.CreationModeEnum.CREATED_CLEAN_NEW, res.mode, "Not clean session must be created");
        // remove it, so that it's tracked in the inner delay queue
        sut.connectionClosed(res.session);
        assertEquals(1, sessionRepository.list().size(), "Not clean session must be persisted");
        // move time forward
        Duration moreThenSessionExpiration = Duration.ofSeconds(customExpirySeconds).plusSeconds(10);
        slidingClock.forward(moreThenSessionExpiration);
        // check the session has been removed
        Awaitility
            .await()
            .atMost(3 * SessionRegistry.EXPIRED_SESSION_CLEANER_TASK_INTERVAL.toMillis(), TimeUnit.MILLISECONDS)
            .until(sessionsList(), Matchers.empty());
    }
}