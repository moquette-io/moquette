/*
 * Copyright (c) 2012-2018 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.moquette.integration;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

import static io.moquette.broker.config.IConfig.DATA_PATH_PROPERTY_NAME;
import static io.moquette.BrokerConstants.DEFAULT_MOQUETTE_STORE_H2_DB_FILENAME;
import static io.moquette.broker.config.IConfig.ENABLE_TELEMETRY_NAME;
import static io.moquette.broker.config.IConfig.PERSISTENCE_ENABLED_PROPERTY_NAME;
import static io.moquette.broker.config.IConfig.PERSISTENT_QUEUE_TYPE_PROPERTY_NAME;
import static io.moquette.broker.config.IConfig.PORT_PROPERTY_NAME;

/**
 * Used to carry integration configurations.
 */
public final class IntegrationUtils {

    /**
     * Creates child folder inside parent
     * @param parent the folder where to insert the new subfolder.
     * @param child name of the subfolder.
     * @return the reference to the newly created directory
     * */
    public static File newFolder(Path parent, String child) throws IOException {
        final Path newPath = parent.resolve(child);
        final File dir = newPath.toFile();
        if (!dir.mkdirs()) {
            throw new IOException("Can't create the new folder path: " + dir.getAbsolutePath());
        }
        return dir;
    }

    public static String tempH2Path(Path tempFolder) {
        return tempFolder.toAbsolutePath() + File.separator + DEFAULT_MOQUETTE_STORE_H2_DB_FILENAME;
    }

    public static Properties prepareTestProperties(String dbPath) {
        Properties testProperties = new Properties();
        testProperties.put(DATA_PATH_PROPERTY_NAME, dbPath);
        testProperties.put(PERSISTENCE_ENABLED_PROPERTY_NAME, "true");
        testProperties.put(PORT_PROPERTY_NAME, "1883");
        testProperties.put(ENABLE_TELEMETRY_NAME, "false");
        testProperties.put(PERSISTENT_QUEUE_TYPE_PROPERTY_NAME, "segmented");
        return testProperties;
    }

    private IntegrationUtils() {
    }

    static void disconnectClient(IMqttClient client) throws MqttException {
        if (client != null && client.isConnected()) {
            client.disconnect();
        }
    }

    static void disconnectClient(IMqttAsyncClient client, IMqttActionListener callback) throws MqttException {
        if (client != null && client.isConnected()) {
            client.disconnect(null, callback);
        }
    }

    static void disconnectClient(IMqttAsyncClient client) throws MqttException {
        if (client != null && client.isConnected()) {
            client.disconnect();
        }
    }
}
