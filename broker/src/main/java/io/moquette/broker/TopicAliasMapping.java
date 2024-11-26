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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Implements a mapping cache so that the binding key -> value is also reversed,
 * it maintains the invariant to be a 1:1 mapping.
 * */
class TopicAliasMapping {

    private final Map<String, Integer> direct = new HashMap<>();
    private final Map<Integer, String> reversed = new HashMap<>();

    void update(String topicName, int topicAlias) {
        Integer previousAlias = direct.put(topicName, topicAlias);
        if (previousAlias != null) {
            reversed.remove(previousAlias);
        }
        reversed.put(topicAlias, topicName);
    }

    Optional<String> topicFromAlias(int topicAlias) {
        return Optional.ofNullable(reversed.get(topicAlias));
    }

    int size() {
        return reversed.size();
    }
}
