/*
 * Copyright (c) 2012-2014 The original author or authors
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
package org.eclipse.moquette.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.lang3.text.StrSubstitutor;
import org.eclipse.moquette.commons.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mosquitto configuration parser.
 * 
 * A line that at the very first has # is a comment
 * Each line has key value format, where the separator used it the space.
 * 
 * @author andrea
 */
class ConfigurationParser {
    
    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationParser.class);
    
    private Properties m_properties = new Properties();
    
    ConfigurationParser() {
    	createDefaults();
    }

    /**
     * Crate a ConfigurationParser merging the default properties with the provided ones.
     * */
    ConfigurationParser(Properties properties) {
    	this();
    	for (Entry<Object, Object> entrySet : properties.entrySet()) {
                m_properties.put(entrySet.getKey(), entrySet.getValue());
    	}
    }
        
    private void createDefaults() {
        m_properties.put(Constants.PORT_PROPERTY_NAME, Integer.toString(Constants.PORT));
        m_properties.put(Constants.HOST_PROPERTY_NAME, Constants.HOST);
        m_properties.put(Constants.WEB_SOCKET_PORT_PROPERTY_NAME, Integer.toString(Constants.WEBSOCKET_PORT));
        m_properties.put(Constants.PASSWORD_FILE_PROPERTY_NAME, "");
        m_properties.put(Constants.PERSISTENT_STORE_PROPERTY_NAME, Constants.DEFAULT_PERSISTENT_PATH);
    }
    
    /**
     * Parse the configuration from file.
     */
    void parse(File file) throws ParseException {
        if (file == null) {
            LOG.warn("parsing NULL file, so fallback on default configuration!");
            return;
        }
        if (!file.exists()) {
            LOG.warn(String.format("parsing not existing file %s, so fallback on default configuration!", file.getAbsolutePath()));
            return;
        }
        try {
            FileReader reader = new FileReader(file);
            parse(reader);
        } catch (FileNotFoundException fex) {
            LOG.warn(String.format("parsing not existing file %s, so fallback on default configuration!", file.getAbsolutePath()), fex);
            return;
        }
    }
    
    /**
     * Parse the configuration 
     * 
     * @throws ParseException if the format is not compliant.
     */
    void parse(Reader reader) throws ParseException {
        if (reader == null) {
            //just log and return default properties
            LOG.warn("parsing NULL reader, so fallback on default configuration!");
            return;
        }
        
        BufferedReader br = new BufferedReader(reader);
        String line;
        try {
            while ((line = br.readLine()) != null) {
                int commentMarker = line.indexOf('#');
                if (commentMarker != -1) {
                    if (commentMarker == 0) {
                        //skip its a comment
                        continue;
                    } else {
                        //it's a malformed comment
                        throw new ParseException(line, commentMarker);
                    }
                } else {
                    if (line.isEmpty() || line.matches("^\\s*$")) {
                        //skip it's a black line
                        continue;
                    }
                    
                    //split till the first space
                    int deilimiterIdx = line.indexOf(' ');
                    final String key = line.substring(0, deilimiterIdx).trim();
                    final String valueRaw = line.substring(deilimiterIdx).trim();
                    final String value = StrSubstitutor.replaceSystemProperties(valueRaw);
                    
                    m_properties.put(key, value);
                }
            }
        } catch (IOException ex) {
            throw new ParseException("Failed to read", 1);
        }
    }
    
    Properties getProperties() {
        return m_properties;
    }
}
