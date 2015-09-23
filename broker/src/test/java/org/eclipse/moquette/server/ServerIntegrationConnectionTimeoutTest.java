package org.eclipse.moquette.server;

import static org.eclipse.moquette.commons.Constants.PERSISTENT_STORE_PROPERTY_NAME;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.eclipse.moquette.interception.InterceptHandler;
import org.eclipse.moquette.interception.messages.InterceptConnectMessage;
import org.eclipse.moquette.server.config.IConfig;
import org.eclipse.moquette.server.config.MemoryConfig;
import org.eclipse.moquette.server.netty.NettyAcceptor;
import org.eclipse.moquette.spi.impl.DelayedConnectSimpleMessaging;
import org.eclipse.moquette.spi.impl.SimpleMessaging;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerIntegrationConnectionTimeoutTest {
    private static final Logger LOG = LoggerFactory.getLogger(ServerIntegrationConnectionTimeoutTest.class);

    Server m_server;
    IConfig m_config;
    private InterceptHandler mockInterceptHandler;

    @Test
    public void shouldNotConnectedConnectionDelayLagerThanConnectionTimeout() throws MqttException, IOException {
        long connectionDelay = 5L;
        long connectionTimeout = 1L;
        startServer(connectionDelay, connectionTimeout);
        
        IMqttToken token1 = connect("client1");
        IMqttToken token2 = connect("client2");
        IMqttToken token3 = connect("client3");
        IMqttToken token4 = connect("client4");

        sleep(200L);

        waitForCompletion(token1);
        waitForCompletion(token2);
        waitForCompletion(token3);
        waitForCompletion(token4);
        
        verify(mockInterceptHandler, never()).onConnect(any(InterceptConnectMessage.class));
    }

    @Test
    public void shouldConnectedConnectionDelaySmallerThanConnectionTimeout() throws MqttException, IOException {
        long connectionDelay = 1L;
        long connectionTimeout = 500L;
        
        startServer(connectionDelay, connectionTimeout);
        
        IMqttToken token1 = connect("client1");
        IMqttToken token2 = connect("client2");
        IMqttToken token3 = connect("client3");
        IMqttToken token4 = connect("client4");

        sleep(200L);

        waitForCompletion(token1);
        waitForCompletion(token2);
        waitForCompletion(token3);
        waitForCompletion(token4);
        
        verify(mockInterceptHandler, times(4)).onConnect(any(InterceptConnectMessage.class));
    }

    private void waitForCompletion(IMqttToken token) throws MqttException {
        if (token == null) {
            return;
        }
        try {
            token.waitForCompletion();
        } catch (Throwable t) {
            LOG.error("token.waitForCompletion() client[{}] {}", token.getClient(), t);
            LOG.error("Throwable : ", t);
        }
    }


    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }

    private IMqttToken connect(String clinetId) throws MqttException {
        IMqttAsyncClient client = new MqttAsyncClient("tcp://127.0.0.1:1883", clinetId, new MemoryPersistence());
        try {
            return client.connect();
        } catch (Throwable t) {
            LOG.error("client.connect() client[{}] {}", clinetId, t);
            LOG.error("Throwable : ", t);
            return null;
        }
    }

    @After
    public void tearDown() throws Exception {
        stopServer();
    }

    private void stopServer() {
        if (m_server != null) { 
            m_server.stopServer();
        }
        File dbFile = new File(m_config.getProperty(PERSISTENT_STORE_PROPERTY_NAME));
        if (dbFile.exists()) {
            dbFile.delete();
        }
    }

    protected void startServer(long connectionDelay, long connectionTimeout) throws IOException {
        mockInterceptHandler = mock(InterceptHandler.class);
        m_server = new DelayedConnectionServer(connectionDelay, connectionTimeout);
        final Properties configProps = IntegrationUtils.prepareTestPropeties();
        m_config = new MemoryConfig(configProps);
        m_server.startServer(m_config);
    }

    private class DelayedConnectionServer extends Server {
        private final long connectionDelay;
        private final long connectionTimeout;
        public DelayedConnectionServer(long connectionDelay, long connectionTimeout) {
            this.connectionDelay = connectionDelay;
            this.connectionTimeout = connectionTimeout;
        }
        @Override
        protected NettyAcceptor createNettyAcceptor() {
            return new NettyAcceptor() {
                @Override
                protected long getConnectTimeout() {
                    return connectionTimeout;
                }
            };
        }
        @Override
        protected SimpleMessaging createSimpleMessaging() {
            return new DelayedConnectSimpleMessaging(connectionDelay) {

                @Override
                protected void initDefaultObservers(List<InterceptHandler> observers) {
                    super.initDefaultObservers(observers);
                    observers.add(mockInterceptHandler);
                }
            };
        }
    }
}
