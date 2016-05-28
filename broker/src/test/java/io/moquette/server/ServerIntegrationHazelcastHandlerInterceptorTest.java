package io.moquette.server;

import io.moquette.BrokerConstants;
import io.moquette.interception.HazelcastInterceptHandler;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Created by mackristof on 13/05/2016.
 */
public class ServerIntegrationHazelcastHandlerInterceptorTest{
    private static final Logger LOG = LoggerFactory.getLogger(ServerIntegrationHazelcastHandlerInterceptorTest.class);

    static MqttClientPersistence s_dataStore;
    static MqttClientPersistence s_pubDataStore;

    Server m_server;
    IMqttClient m_client;
    IMqttClient m_publisher;
    MessageCollector m_messagesCollector;
    IConfig m_config;



    @BeforeClass
    public static void beforeTests() throws NoSuchAlgorithmException, SQLException, ClassNotFoundException {
        String tmpDir = System.getProperty("java.io.tmpdir");
        s_dataStore = new MqttDefaultFilePersistence(tmpDir);
        s_pubDataStore = new MqttDefaultFilePersistence(tmpDir + File.separator + "publisher");

    }

    protected void startServer() throws IOException {
        m_server = new Server();
        final Properties configProps = addHazelCastConf(IntegrationUtils.prepareTestProperties());
        m_config = new MemoryConfig(configProps);
        m_server.startServer(m_config);
    }


    private void stopServer() {
        m_server.stopServer();
        IntegrationUtils.cleanPersistenceFile(m_config);
    }

    private Properties addHazelCastConf(Properties properties) {
        properties.put(BrokerConstants.INTERCEPT_HANDLER_PROPERTY_NAME, HazelcastInterceptHandler.class.getCanonicalName());
        return properties;
    }

    @Before
    public void setUp() throws Exception {
//        String dbPath = IntegrationUtils.localMapDBPath();
//        IntegrationUtils.cleanPersistenceFile(dbPath);
//
//        startServer();

        m_client = new MqttClient("tcp://dev:1884", "TestClient", s_dataStore);
        m_messagesCollector = new MessageCollector();
        m_client.setCallback(m_messagesCollector);

        m_client.connect();

        m_publisher = new MqttClient("tcp://dev:1883", "Publisher", s_pubDataStore);
        m_publisher.connect();
    }

    @After
    public void tearDown() throws Exception {
        if (m_client != null && m_client.isConnected()) {
            m_client.disconnect();
        }

        if (m_publisher != null && m_publisher.isConnected()) {
            m_publisher.disconnect();
        }

        //stopServer();
    }

    @AfterClass
    public static void shutdown(){

    }

    @Test
    public void checkPublishOnHazelCastQueue() throws Exception {

        LOG.info("*** checkPublishOnHazelCastQueue ***");
        m_client.subscribe("/topic", 0);

        m_publisher.publish("/topic", "Hello world MQTT QoS0".getBytes(), 0, false);
        MqttMessage message = m_messagesCollector.getMessage(true);
        assertEquals("Hello world MQTT QoS0", message.toString());
        assertEquals(0, message.getQos());
        Thread.sleep(5000);


    }



}
