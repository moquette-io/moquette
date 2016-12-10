/*
 * Copyright (c) 2012-2015 The original author or authors
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
package io.moquette.server.config;

import io.moquette.BrokerConstants;
import org.junit.Before;
import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;
import java.text.ParseException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author andrea
 */
public class ConfigurationParserTest {

    ConfigurationParser m_parser;
    
    @Before
    public void setUp() {
        m_parser = new ConfigurationParser();
    }
    
    @Test
    public void checkDefaultOptions() {
        Properties props = m_parser.getProperties();
        
        //verifyDefaults(props);
        assertTrue(props.isEmpty());
    }
    
    @Test
    public void parseEmpty() throws ParseException {
        Reader conf = new StringReader("  ");
        m_parser.parse(conf);
        
        //Verify
//        verifyDefaults(m_parser.getProperties());
        assertTrue(m_parser.getProperties().isEmpty());
    }
    
    @Test
    public void parseValidComment() throws ParseException {
        Reader conf = new StringReader("#simple comment");
        m_parser.parse(conf);
        
        //Verify
//        verifyDefaults(m_parser.getProperties());
        assertTrue(m_parser.getProperties().isEmpty());
    }
    
    @Test(expected = ParseException.class)
    public void parseInvalidComment() throws ParseException {
        Reader conf = new StringReader(" #simple comment");
        m_parser.parse(conf);
    } 
    
    @Test
    public void parseSingleVariable() throws ParseException {
        Reader conf = new StringReader("port 1234");
        m_parser.parse(conf);
        
        //Verify
        assertEquals("1234", m_parser.getProperties().getProperty("port"));
    }
    
    @Test
    public void parseCompleteFile() throws ParseException {
        String content = "# This is initial m_config format \r\n"
                + "  \r\n"
                + "port 1234 \r\n"
                + "host   localhost \r\n"
                + "fake  multi word string property\r\n";
        Reader conf = new StringReader(content);
        m_parser.parse(conf);
        
        //Verify
        Properties props = m_parser.getProperties();
        assertEquals("1234", props.getProperty("port"));
        assertEquals("localhost", props.getProperty("host"));
        assertEquals("multi word string property", props.getProperty("fake"));
    }
    
    /**
     * Helper method to verify default options.
     */
    private void verifyDefaults(Properties props) {
        assertEquals(BrokerConstants.PORT, Integer.parseInt(props.getProperty(BrokerConstants.PORT_PROPERTY_NAME)));
        assertEquals(BrokerConstants.HOST, props.getProperty(BrokerConstants.HOST_PROPERTY_NAME));
        assertNull(props.getProperty(BrokerConstants.WEB_SOCKET_PORT_PROPERTY_NAME));
    }
}