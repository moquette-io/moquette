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

package io.moquette.broker.security;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.moquette.broker.subscriptions.Topic;
import java.text.ParseException;
import static org.junit.jupiter.api.Assertions.*;

public class AuthorizationsCollectorTest {

    private static final Authorization RW_ANEMOMETER = new Authorization(new Topic("/weather/italy/anemometer"));
    private static final Authorization R_ANEMOMETER = new Authorization(
            new Topic("/weather/italy/anemometer"),
            Authorization.Permission.READ);
    private static final Authorization W_ANEMOMETER = new Authorization(
            new Topic("/weather/italy/anemometer"),
            Authorization.Permission.WRITE);

    private AuthorizationsCollector authorizator;

    @BeforeEach
    public void setUp() {
        authorizator = new AuthorizationsCollector();
    }

    @Test
    public void testParseAuthLineValid() throws ParseException {
        Authorization authorization = authorizator.parseAuthLine("topic /weather/italy/anemometer");

        // Verify
        assertEquals(RW_ANEMOMETER, authorization);
    }

    @Test
    public void testParseAuthLineValid_read() throws ParseException {
        Authorization authorization = authorizator.parseAuthLine("topic read /weather/italy/anemometer");

        // Verify
        assertEquals(R_ANEMOMETER, authorization);
    }

    @Test
    public void testParseAuthLineValid_write() throws ParseException {
        Authorization authorization = authorizator.parseAuthLine("topic write /weather/italy/anemometer");

        // Verify
        assertEquals(W_ANEMOMETER, authorization);
    }

    @Test
    public void testParseAuthLineValid_readwrite() throws ParseException {
        Authorization authorization = authorizator.parseAuthLine("topic readwrite /weather/italy/anemometer");

        // Verify
        assertEquals(RW_ANEMOMETER, authorization);
    }

    @Test
    public void testParseAuthLineValid_topic_with_space() throws ParseException {
        Authorization expected = new Authorization(new Topic("/weather/eastern italy/anemometer"));
        Authorization authorization = authorizator.parseAuthLine("topic readwrite /weather/eastern italy/anemometer");

        // Verify
        assertEquals(expected, authorization);
    }

    @Test
    public void testParseAuthLineValid_invalid() {
        assertThrows(ParseException.class, () -> authorizator.parseAuthLine("topic faker /weather/italy/anemometer"));
    }

    @Test
    public void testCanWriteSimpleTopic() throws ParseException {
        authorizator.parse("topic write /sensors");

        // verify
        assertTrue(authorizator.canWrite(new Topic("/sensors"), "", ""));
    }

    @Test
    public void testCanReadSimpleTopic() throws ParseException {
        authorizator.parse("topic read /sensors");

        // verify
        assertTrue(authorizator.canRead(new Topic("/sensors"), "", ""));
    }

    @Test
    public void testCanReadWriteMixedSimpleTopic() throws ParseException {
        authorizator.parse("topic write /sensors");
        authorizator.parse("topic read /sensors/anemometer");

        // verify
        assertTrue(authorizator.canWrite(new Topic("/sensors"), "", ""));
        assertFalse(authorizator.canRead(new Topic("/sensors"), "", ""));
    }

    @Test
    public void testCanWriteMultiMatherTopic() throws ParseException {
        authorizator.parse("topic write /sensors/#");

        // verify
        assertTrue(authorizator.canWrite(new Topic("/sensors/anemometer/wind"), "", ""));
    }

    @Test
    public void testCanWriteSingleMatherTopic() throws ParseException {
        authorizator.parse("topic write /sensors/+");

        // verify
        assertTrue(authorizator.canWrite(new Topic("/sensors/anemometer"), "", ""));
    }

    @Test
    public void testCanWriteUserTopic() throws ParseException {
        authorizator.parse("user john");
        authorizator.parse("topic write /sensors");

        // verify
        assertTrue(authorizator.canWrite(new Topic("/sensors"), "john", ""));
        assertFalse(authorizator.canWrite(new Topic("/sensors"), "jack", ""));
    }

    @Test
    public void testPatternClientLineACL() throws ParseException {
        authorizator.parse("pattern read /weather/italy/%c");

        // Verify
        assertTrue(authorizator.canRead(new Topic("/weather/italy/anemometer1"), "", "anemometer1"));
    }

    @Test
    public void testPatternClientAndUserLineACL() throws ParseException {
        authorizator.parse("pattern read /weather/%u/%c");

        // Verify
        assertTrue(authorizator.canRead(new Topic("/weather/italy/anemometer1"), "italy", "anemometer1"));
    }

    @Test
    public void givenPatternACLWhichAcceptsClientIDPlaceholderWhenProvidedClientIDContainsWildcardThenMustNotBeMatched() throws ParseException {
        // Per-tenant isolation: each client may only touch /weather/italy/<its-clientId>.
        authorizator.parse("pattern read /weather/italy/%c");
        authorizator.parse("pattern write /weather/italy/%c");

        // Control: a legitimate client reaches only its own topic.
        assertTrue(authorizator.canRead(new Topic("/weather/italy/anemometer1"), "", "anemometer1"));

        // A client that connects with a wildcard client id must NOT read/write another client's topic:
        // '+' would substitute the rule to /weather/italy/+ and '#' to /weather/italy/#.
        assertFalse(authorizator.canRead(new Topic("/weather/italy/victim"), "", "+"));
        assertFalse(authorizator.canRead(new Topic("/weather/italy/victim"), "", "#"));
        assertFalse(authorizator.canWrite(new Topic("/weather/italy/victim"), "", "+"));
        assertFalse(authorizator.canWrite(new Topic("/weather/italy/victim"), "", "#"));
    }

    @Test
    public void givenPatternACLWhichAcceptsUsernamePlaceholderWhenProvidedUsernameContainsWildcardThenMustNotBeMatched() throws ParseException {
        authorizator.parse("pattern read /weather/%u/data");
        // Control.
        assertTrue(authorizator.canRead(new Topic("/weather/italy/data"), "italy", ""));
        // A wildcard username must not broaden the grant to another tenant's data.
        assertFalse(authorizator.canRead(new Topic("/weather/victim/data"), "+", ""));
    }

    @Test
    public void givenPatternACLWhichAcceptsClientIDPlaceholderWhenProvidedClientIDContainsMultilevelWildcardThenDoesntThrowError() throws ParseException {
        // A '#' client id substituted into /weather/italy/%c/# yields the invalid filter
        // /weather/italy/#/#, which previously produced a Topic with no tokens and threw a
        // NullPointerException in Topic.match(). It must now be handled cleanly (denied, no throw).
        authorizator.parse("pattern read /weather/italy/%c/#");
        assertDoesNotThrow(() -> authorizator.canRead(new Topic("/weather/italy/victim/inner"), "", "#"));
        assertFalse(authorizator.canRead(new Topic("/weather/italy/victim/inner"), "", "#"));
    }
}
