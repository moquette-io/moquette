package io.moquette.integration.mqtt5;

import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.integration.IntegrationUtils;
import io.moquette.testclient.Client;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttProperties;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

import static io.moquette.BrokerConstants.INFLIGHT_WINDOW_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class ConnectAckTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectAckTest.class);

    Server broker;
    IConfig config;

    @TempDir
    Path tempFolder;
    private String dbPath;

    protected void startServer(String dbPath) throws IOException {
        broker = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
        config = new MemoryConfig(configProps);
        broker.startServer(config);
    }

    @BeforeAll
    public static void beforeTests() {
        Awaitility.setDefaultTimeout(Durations.ONE_SECOND);
    }

    @BeforeEach
    public void setUp() throws Exception {
        dbPath = IntegrationUtils.tempH2Path(tempFolder);
        startServer(dbPath);
    }

    @AfterEach
    public void tearDown() throws Exception {
        stopServer();
    }

    private void stopServer() {
        broker.stopServer();
    }

    @Test
    public void testReceiveMaximum() {
        final Client lowClient = new Client("localhost").clientId("client");
        final MqttConnAckMessage connAck = lowClient.connectV5();
        assertEquals(MqttConnectReturnCode.CONNECTION_ACCEPTED, connAck.variableHeader().connectReturnCode(), "Client connected");

        final MqttProperties ackProps = connAck.variableHeader().properties();
        final MqttProperties.MqttProperty<Integer> property = ackProps.getProperty(MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM.value());
        assertEquals(INFLIGHT_WINDOW_SIZE, property.value(), "Receive maximum property must equals flight window size.");
    }
}
