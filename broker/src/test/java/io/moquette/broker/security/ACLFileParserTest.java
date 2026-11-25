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

import org.junit.jupiter.api.Test;
import io.moquette.broker.subscriptions.Topic;
import java.io.Reader;
import java.io.StringReader;
import java.text.ParseException;
import static org.junit.jupiter.api.Assertions.*;

public class ACLFileParserTest {

    @Test
    public void testParseEmpty() throws ParseException {
        Reader conf = new StringReader("  ");
        AuthorizationsCollector authorizations = ACLFileParser.parse(conf);

        // Verify
        assertTrue(authorizations.isEmpty());
    }

    @Test
    public void testParseValidComment() throws ParseException {
        Reader conf = new StringReader("#simple comment");
        AuthorizationsCollector authorizations = ACLFileParser.parse(conf);

        // Verify
        assertTrue(authorizations.isEmpty());
    }

    @Test
    public void testParseInvalidPaddedComment() throws ParseException {
        Reader conf = new StringReader(" #simple comment");
        assertThrows(ParseException.class, () -> ACLFileParser.parse(conf));

        // Verify
//        assertTrue(authorizations.isEmpty());
    }

    @Test
    public void testParseSingleLineACL() throws ParseException {
        Reader conf = new StringReader("topic /weather/italy/anemometer");
        AuthorizationsCollector authorizations = ACLFileParser.parse(conf);

        // Verify
        assertTrue(authorizations.canRead(new Topic("/weather/italy/anemometer"), "", ""));
        assertTrue(authorizations.canWrite(new Topic("/weather/italy/anemometer"), "", ""));
    }

    @Test
    public void testParseValidEndLineComment() throws ParseException {
        Reader conf = new StringReader("topic /weather/italy/anemometer #simple comment");
        AuthorizationsCollector authorizations = ACLFileParser.parse(conf);

        // Verify
        assertTrue(authorizations.canRead(new Topic("/weather/italy/anemometer"), "", ""));
        assertTrue(authorizations.canWrite(new Topic("/weather/italy/anemometer"), "", ""));
    }

    @Test
    public void testParseValidPoundTopicWithEndLineComment() throws ParseException {
        Reader conf = new StringReader("topic /weather/italy/anemometer/# #simple comment");
        AuthorizationsCollector authorizations = ACLFileParser.parse(conf);

        // Verify
        assertTrue(authorizations.canRead(new Topic("/weather/italy/anemometer/#"), "", ""));
        assertTrue(authorizations.canWrite(new Topic("/weather/italy/anemometer/#"), "", ""));
    }

    @Test
    public void testParseValidPlusTopicWithEndLineComment() throws ParseException {
        Reader conf = new StringReader("topic /weather/+/anemometer #simple comment");
        AuthorizationsCollector authorizations = ACLFileParser.parse(conf);

        // Verify
        assertTrue(authorizations.canRead(new Topic("/weather/+/anemometer"), "", ""));
        assertTrue(authorizations.canWrite(new Topic("/weather/+/anemometer"), "", ""));
    }

    @Test
    public void givenReadRuleWithTrailingMultiLevelWildcard_whenParsed_thenWildcardIsKept() throws ParseException {
        Reader conf = new StringReader("user alice\ntopic read alice/#");
        AuthorizationsCollector authorizations = ACLFileParser.parse(conf);

        assertTrue(authorizations.canRead(new Topic("alice/a"), "alice", ""));
        assertTrue(authorizations.canRead(new Topic("alice/a/b/c"), "alice", ""));
        assertFalse(authorizations.canWrite(new Topic("alice/a"), "alice", ""));
        assertFalse(authorizations.canRead(new Topic("bob/a"), "alice", ""));
    }

    @Test
    public void givenReadWriteRuleWithBareMultiLevelWildcard_whenParsed_thenAllTopicsAreGranted() throws ParseException {
        Reader conf = new StringReader("user alice\ntopic readwrite #");
        AuthorizationsCollector authorizations = ACLFileParser.parse(conf);

        assertTrue(authorizations.canRead(new Topic("any/topic"), "alice", ""));
        assertTrue(authorizations.canWrite(new Topic("any/topic"), "alice", ""));
    }

    @Test
    public void givenRuleWithTrailingWildcardAndEndLineComment_whenParsed_thenWildcardIsKeptAndCommentDropped() throws ParseException {
        Reader conf = new StringReader("user alice\ntopic read alice/# # trailing comment");
        AuthorizationsCollector authorizations = ACLFileParser.parse(conf);

        assertTrue(authorizations.canRead(new Topic("alice/a/b"), "alice", ""));
        assertFalse(authorizations.canRead(new Topic("bob/a"), "alice", ""));
    }

    @Test
    public void givenRuleWithPlusWildcardAndSpacedEndLineComment_whenParsed_thenWildcardIsKeptAndCommentDropped() throws ParseException {
        Reader conf = new StringReader("user alice\ntopic read alice/+/x   # comment");
        AuthorizationsCollector authorizations = ACLFileParser.parse(conf);

        assertTrue(authorizations.canRead(new Topic("alice/a/x"), "alice", ""));
        assertFalse(authorizations.canRead(new Topic("alice/a/y"), "alice", ""));
    }

    @Test
    public void givenPatternRuleWithTrailingWildcard_whenParsed_thenWildcardIsKept() throws ParseException {
        Reader conf = new StringReader("pattern write devices/%c/#");
        AuthorizationsCollector authorizations = ACLFileParser.parse(conf);

        assertTrue(authorizations.canWrite(new Topic("devices/client1/a/b"), "", "client1"));
        assertFalse(authorizations.canWrite(new Topic("devices/client2/a"), "", "client1"));
    }
}
