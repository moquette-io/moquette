package io.moquette.integration;

import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

public class AbstractServerIntegrationMixin {

    @TempDir
    Path tempFolder;
    private String dbPath;

    Server m_server;
    IConfig m_config;

    /**
     * Call this method in every @BeforeEach as first method call.
     * */
    protected void setUp() throws Exception {
        dbPath = IntegrationUtils.tempH2Path(tempFolder);
        startServer(dbPath);
    }

    /**
     * Call this method in every @AfterEach as last method call.
     * */
    public void tearDown() throws Exception {
        stopServer();
    }

    protected void stopServer() {
        m_server.stopServer();
    }

    protected void startServer(String dbPath) throws IOException {
        m_server = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
        m_config = new MemoryConfig(configProps);
        m_server.startServer(m_config);
    }
}
