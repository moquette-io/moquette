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

package io.moquette.spi.impl.subscriptions;

import java.io.IOException;
import java.util.function.Consumer;
import io.moquette.persistence.MemoryStorageService;
import io.moquette.spi.ISessionsStore;
import io.netty.handler.codec.mqtt.MqttQoS;

public final class PerformanceTest {

    // Set CPU governor and max frequency to lowest value for this test
    /* debian:
       cpufreq-set -g performance -u 800MHz -c 0
       cpufreq-set -g performance -u 800MHz -c 1
       cpufreq-set -g performance -u 800MHz -c 2
       cpufreq-set -g performance -u 800MHz -c 3
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        SubscriptionsDirectory store = new SubscriptionsDirectory();
        MemoryStorageService memStore = new MemoryStorageService(null, null);
        ISessionsStore aSessionsStore = memStore.sessionsStore();
        store.init(aSessionsStore);

        int times = 500;

        int users = 500;
        int devices = 5;
        int values = 3;
        int subs = 2;
        int topiccount = users * devices * values * subs;

        Topic[] topics = new Topic[topiccount];
        for (int i = 0; i < topiccount; i++) {
            topics[i] = new Topic(
                    (i % users) + "/" + ((i / users) % devices) + "/" + ((i / (users * devices)) % values) + "/"
                            + ((i / (users * devices * values)) % subs));

            Subscription sub = new Subscription("CLI_ID_" + (i % users), topics[i], MqttQoS.AT_MOST_ONCE);
            aSessionsStore.subscriptionStore().addNewSubscription(sub);
            store.add(sub.asClientTopicCouple());
        }

        System.out.println("Store size: " + store.size() + " " + topiccount);

        // warm up
        for (int i = 0; i < topiccount; i++) {
            store.matches(topics[i]);
        }

        for (int i = 0; i < topiccount; i++) {
            store.matches2(topics[i]);
        }

        System.out.println("press return for start");
        System.in.read();

        // test new
        test(times, topiccount, i -> store.matches(topics[i]));

        // test old
        test(times, topiccount, i -> store.matches2(topics[i]));
    }

    private static void test(int times, int topiccount, Consumer<Integer> c) throws InterruptedException {
        Thread.sleep(3000);
        long min = Long.MAX_VALUE;
        for (int j = 0; j < times; j++) {

            System.gc();
            Thread.yield();

            long time = System.currentTimeMillis();
            for (int i = 0; i < topiccount; i++) {
                c.accept(i);
            }
            long time2 = System.currentTimeMillis() - time;
            min = Math.min(min, time2);
            System.out.println(".." + j + ": " + time2 + " min:" + min);
        }

        System.out.println("min:" + min);
    }

    private PerformanceTest() {
    }
}
