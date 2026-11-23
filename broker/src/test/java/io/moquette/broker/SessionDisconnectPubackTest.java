package io.moquette.broker;

import io.moquette.broker.security.PermitAllAuthorizatorPolicy;
import io.moquette.broker.subscriptions.CTrieSubscriptionDirectory;
import io.moquette.broker.subscriptions.ISubscriptionsDirectory;
import io.moquette.interception.BrokerInterceptor;
import io.moquette.interception.InterceptHandler;
import io.moquette.interception.messages.*;
import io.moquette.metrics.MetricsProvider;
import io.moquette.metrics.MetricsProviderNull;
import io.moquette.persistence.MemorySubscriptionsRepository;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.moquette.BrokerConstants.NO_BUFFER_FLUSH;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * A PUBACK for an inflight QoS1 message received after DISCONNECT must not kill the session event loop.
 */
public class SessionDisconnectPubackTest {

    private static final String CLIENT_ID = "persistent-sub";
    private static final String TOPIC = "/poc/disconnect-puback";
    private CountDownLatch loopErrorSeen;
    private BrokerInterceptor interceptor;
    private ScheduledExecutorService scheduler;
    private PostOffice postOffice;
    private MQTTConnection connection;

    @BeforeEach
    public void setUp() {
        loopErrorSeen = new CountDownLatch(1);
        interceptor = new BrokerInterceptor(Collections.singletonList(new ErrorCapture(loopErrorSeen)));
        scheduler = Executors.newScheduledThreadPool(1);
        
        ISubscriptionsDirectory subscriptions = new CTrieSubscriptionDirectory();
        subscriptions.init(new MemorySubscriptionsRepository());
        MetricsProvider metrics = new MetricsProviderNull();
        Authorizator authorizator = new Authorizator(new PermitAllAuthorizatorPolicy());
        SessionEventLoopGroup loops = new SessionEventLoopGroup(interceptor, 1024, metrics);
        ISessionsRepository sessions = MQTTConnectionPublishTest.memorySessionsRepository();
        SessionRegistry sessionsRegistry = new SessionRegistry(subscriptions, sessions, new MemoryQueueRepository(),
            authorizator, scheduler, loops, metrics);
        postOffice = new PostOffice(subscriptions, new MemoryRetainedRepository(), sessionsRegistry, sessions,
            interceptor, authorizator, loops, metrics);
        MockAuthenticator authenticator = new MockAuthenticator(Collections.singleton(CLIENT_ID), Collections.emptyMap());
        BrokerConfiguration brokerConfig = new BrokerConfiguration(true, true, false, NO_BUFFER_FLUSH);
        connection = new MQTTConnection(new EmbeddedChannel(), brokerConfig, authenticator, sessionsRegistry, postOffice);
    }

    @AfterEach
    private void tearDown() {
        scheduler.shutdownNow();
        interceptor.stop();
    }

    @Test
    public void pubackAfterDisconnectDoesNotCrashSessionLoop() throws Exception {
        ConnectionTestUtils.connect(connection, ConnectionTestUtils.buildConnectNotClean(CLIENT_ID));

        MqttSubscribeMessage subscribeMsg = MqttMessageBuilders.subscribe()
            .addSubscription(MqttQoS.AT_LEAST_ONCE, TOPIC)
            .messageId(1)
            .build();
        postOffice.subscribeClientToTopics(subscribeMsg, CLIENT_ID, null, connection);
        // consumes the SUBACK form the client side
        ((EmbeddedChannel) connection.channel).readOutbound();

        // send a PUBLISH from the broker to the client
        MqttPublishMessage publish = MqttMessageBuilders.publish()
            .topicName(TOPIC)
            .qos(MqttQoS.AT_LEAST_ONCE)
            .payload(Unpooled.copiedBuffer("queued".getBytes(UTF_8)))
            .build();
        postOffice.internalPublish(publish).completableFuture().get(5, TimeUnit.SECONDS);
        
        // Verify the client receive the PUBLISH
        MqttPublishMessage outbound = ((EmbeddedChannel) connection.channel).flushOutbound().readOutbound();
        assertNotNull(outbound, "expected an inflight QoS1 publish");
        int packetId = outbound.variableHeader().packetId();
        ReferenceCountUtil.release(outbound);
        
        // send a DISCONNECT to the broker
        connection.handleMessage(MqttMessageBuilders.disconnect().build());
        
        // send a PUBACK to the broker
        MqttFixedHeader pubackHeader = new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE,
            false, 0);
        connection.handleMessage(new MqttMessage(pubackHeader, MqttMessageIdVariableHeader.from(packetId)));

        assertFalse(loopErrorSeen.await(1, TimeUnit.SECONDS), "session loop must not fail");
    }

    private static final class ErrorCapture implements InterceptHandler {
        private final CountDownLatch seen;

        ErrorCapture(CountDownLatch seen) {
            this.seen = seen;
        }

        @Override public String getID() { return "loop-error-capture"; }
        @Override public Class<?>[] getInterceptedMessageTypes() { return new Class<?>[]{InterceptExceptionMessage.class}; }
        @Override public void onConnect(InterceptConnectMessage msg) { }
        @Override public void onDisconnect(InterceptDisconnectMessage msg) { }
        @Override public void onConnectionLost(InterceptConnectionLostMessage msg) { }
        @Override public void onPublish(InterceptPublishMessage msg) { msg.getPayload().release(); }
        @Override public void onSubscribe(InterceptSubscribeMessage msg) { }
        @Override public void onUnsubscribe(InterceptUnsubscribeMessage msg) { }
        @Override public void onMessageAcknowledged(InterceptAcknowledgedMessage msg) { }
        @Override public void onSessionLoopError(Throwable error) { seen.countDown(); }
    }
}
