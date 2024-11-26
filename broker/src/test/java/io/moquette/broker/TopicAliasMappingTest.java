/*
 *
 * Copyright (c) 2012-2024 The original author or authors
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
 *
 */

package io.moquette.broker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicAliasMappingTest {

    private TopicAliasMapping sut;

    @BeforeEach
    void setUp() {
        sut = new TopicAliasMapping();
    }

    private static void verifyTopicName(String expected, Optional<String> topicName) {
        assertTrue(topicName.isPresent());
        assertEquals(expected, topicName.get());
    }

    @Test
    public void givenAnEmptyMappingWhenNewBindingIsRequestedThenShouldBeRetrievable() {
        sut.update("sensors/temperature", 12);

        Optional<String> topicName = sut.topicFromAlias(12);
        verifyTopicName("sensors/temperature", topicName);
        assertEquals(1, sut.size());
    }

    @Test
    public void givenAnExistingBindingWhenNewBindingWithSameTopicNameIsRequestedThenAliasIsUpdated() {
        // first binding
        sut.update("sensors/temperature", 12);

        // second update with different alias
        sut.update("sensors/temperature", 22);
        assertEquals(1, sut.size());

        Optional<String> topicName = sut.topicFromAlias(22);
        verifyTopicName("sensors/temperature", topicName);
    }

    @Test
    public void givenAnExistingBindingWhenNewBindingWithDifferentTopicNameAndSameTopicAliasIsRequestedThenAliasIsUpdated() {
        // first binding
        sut.update("sensors/temperature", 12);

        // second update with different name
        sut.update("finance/quotes", 12);
        assertEquals(1, sut.size());

        Optional<String> topicName = sut.topicFromAlias(12);
        verifyTopicName("finance/quotes", topicName);
    }
}
