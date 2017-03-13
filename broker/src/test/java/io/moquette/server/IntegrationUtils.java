/*
 * Copyright (c) 2012-2017 The original author or authorsgetRockQuestions()
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

package io.moquette.server;

import io.moquette.server.config.IConfig;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import static io.moquette.BrokerConstants.DEFAULT_MOQUETTE_STORE_MAP_DB_FILENAME;
import static io.moquette.BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME;
import static io.moquette.BrokerConstants.PORT_PROPERTY_NAME;
import static org.junit.Assert.assertFalse;

/**
 * Used to carry integration configurations.
 *
 * Created by andrea on 4/7/15.
 */
public class IntegrationUtils {

    static String localMapDBPath() {
        String currentDir = System.getProperty("user.dir");
        return currentDir + File.separator + DEFAULT_MOQUETTE_STORE_MAP_DB_FILENAME;
    }

    static String localClusterMapDBPath(int port) {
        String currentDir = System.getProperty("user.dir");
        return currentDir + File.separator + port + DEFAULT_MOQUETTE_STORE_MAP_DB_FILENAME;
    }

    public static Properties prepareTestProperties() {
        Properties testProperties = new Properties();
        testProperties.put(PERSISTENT_STORE_PROPERTY_NAME, IntegrationUtils.localMapDBPath());
        testProperties.put(PORT_PROPERTY_NAME, "1883");
        return testProperties;
    }

    public static Properties prepareTestClusterProperties(int port) {
        Properties testProperties = new Properties();
        testProperties.put(PERSISTENT_STORE_PROPERTY_NAME, IntegrationUtils.localClusterMapDBPath(port));
        testProperties.put(PORT_PROPERTY_NAME, Integer.toString(port));
        return testProperties;
    }

    public static void cleanPersistenceFile(IConfig config) {
        String fileName = config.getProperty(PERSISTENT_STORE_PROPERTY_NAME);
        cleanPersistenceFile(fileName);
    }
    
    /**
     * Deletes a directory recursively. 
     *
     * @param directory  directory to delete
     * @throws IOException in case deletion is unsuccessful
     */
    public static void deleteDirectory(File directory) throws IOException {
        if (!directory.exists()) {
            return;
        }

        
        cleanDirectory(directory);        

        if (!directory.delete()) {
            String message =
                "Unable to delete directory " + directory + ".";
            throw new IOException(message);
        }
    }
    
    /**
     * Cleans a directory without deleting it.
     *
     * @param directory directory to clean
     * @throws IOException in case cleaning is unsuccessful
     */
    public static void cleanDirectory(File directory) throws IOException {
        if (!directory.exists()) {
            String message = directory + " does not exist";
            throw new IllegalArgumentException(message);
        }

        if (!directory.isDirectory()) {
            String message = directory + " is not a directory";
            throw new IllegalArgumentException(message);
        }

        File[] files = directory.listFiles();
        if (files == null) {  // null if security restricted
            throw new IOException("Failed to list contents of " + directory);
        }

        IOException exception = null;
        for (File file : files) {
            try {
                forceDelete(file);
            } catch (IOException ioe) {
                exception = ioe;
            }
        }

        if (null != exception) {
            throw exception;
        }
    }

    /**
     * Deletes a file. If file is a directory, delete it and all sub-directories.
     * <p>
     * The difference between File.delete() and this method are:
     * <ul>
     * <li>A directory to be deleted does not have to be empty.</li>
     * <li>You get exceptions when a file or directory cannot be deleted.
     *      (java.io.File methods returns a boolean)</li>
     * </ul>
     *
     * @param file  file or directory to delete, must not be {@code null}
     * @throws NullPointerException if the directory is {@code null}
     * @throws FileNotFoundException if the file was not found
     * @throws IOException in case deletion is unsuccessful
     */
    public static void forceDelete(File file) throws IOException {
        if (file.isDirectory()) {
            deleteDirectory(file);
        } else {
            boolean filePresent = file.exists();
            if (!file.delete()) {
                if (!filePresent){
                    throw new FileNotFoundException("File does not exist: " + file);
                }
                String message =
                    "Unable to delete file: " + file;
                throw new IOException(message);
            }
        }
    }
    
    public static void cleanPersistenceFile(String fileName) {
        File dbFile = new File(fileName);
        if (dbFile.exists()) {
            dbFile.delete();
            new File(fileName + ".p").delete();
            new File(fileName + ".t").delete();
        }
        assertFalse(dbFile.exists());
    }
}
