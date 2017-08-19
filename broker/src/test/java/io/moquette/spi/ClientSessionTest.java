/*
 * Copyright (c) 2012-2017 The original author or authors
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

package io.moquette.spi;

import io.moquette.persistence.MemoryStorageService;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import io.moquette.spi.impl.subscriptions.*;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import static io.netty.handler.codec.mqtt.MqttQoS.EXACTLY_ONCE;
import static org.junit.Assert.assertEquals;

public class ClientSessionTest {

    ClientSession session1;
    ClientSession session2;
    ISessionsStore sessionsStore;
    ISubscriptionsDirectory store;

    @Before
    public void setUp() {
        store = new CTrieSubscriptionDirectory();
        IConfig config = new MemoryConfig(System.getProperties());
        MemoryStorageService storageService = new MemoryStorageService(config, null);
        storageService.initStore();
        this.sessionsStore = storageService.sessionsStore();
        store.init(sessionsStore);

        session1 = sessionsStore.createNewSession("SESSION_ID_1", true);
        session2 = sessionsStore.createNewSession("SESSION_ID_2", true);
    }

    @Test
    public void overridingSubscriptions() {
        // Subscribe on /topic with QOSType.MOST_ONE
        Subscription oldSubscription = new Subscription(session1.clientID, new Topic("/topic"), AT_MOST_ONCE);
        session1.subscribe(oldSubscription);
        store.add(oldSubscription.asClientTopicCouple());

        // Subscribe on /topic again that overrides the previous subscription.
        Subscription overrindingSubscription = new Subscription(session1.clientID, new Topic("/topic"), EXACTLY_ONCE);
        session1.subscribe(overrindingSubscription);
        store.add(overrindingSubscription.asClientTopicCouple());

        // Verify
        List<Subscription> subscriptions = store.matches(new Topic("/topic"));
        assertEquals(1, subscriptions.size());
        Subscription sub = subscriptions.get(0);
        assertEquals(overrindingSubscription.getRequestedQos(), sub.getRequestedQos());
    }
}
