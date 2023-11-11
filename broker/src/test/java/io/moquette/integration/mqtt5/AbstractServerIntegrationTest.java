package io.moquette.integration.mqtt5;

import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.integration.IntegrationUtils;
import io.moquette.testclient.Client;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

public abstract class AbstractServerIntegrationTest {
    Server broker;
    IConfig config;

    @TempDir
    Path tempFolder;
    protected String dbPath;

    Client lowLevelClient;

    public abstract String clientName();

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

        lowLevelClient = new Client("localhost").clientId(clientName());
    }

    @AfterEach
    public void tearDown() throws Exception {
        stopServer();
    }

    protected void stopServer() {
        broker.stopServer();
    }
}
