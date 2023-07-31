/*
 * Copyright (c) 2012-2023 The original author or authors
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
package io.moquette.broker.subscriptions;

import static io.moquette.broker.subscriptions.Topic.asTopic;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CTrieSpeedTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CTrieSpeedTest.class.getName());

    private static final int MAX_DURATION_S = 5 * 60;
    private static final int CHECK_INTERVAL = 50_000;
    private static final int TOTAL_SUBSCRIPTIONS = 500_000;

    static Subscription clientSubOnTopic(String clientID, String topicName) {
        return new Subscription(clientID, asTopic(topicName), null);
    }

    @Test
    @Timeout(value = MAX_DURATION_S)
    public void testManyClientsFewTopics() {
        List<Subscription> subscriptionList = prepareSubscriptionsManyClientsFewTopic();
        createSubscriptions(subscriptionList);
    }

    @Test
    @Timeout(value = MAX_DURATION_S)
    public void testFlat() {
        List<Subscription> results = prepareSubscriptionsFlat();
        createSubscriptions(results);
    }

    @Test
    @Timeout(value = MAX_DURATION_S)
    public void testDeep() {
        List<Subscription> results = prepareSubscriptionsDeep();
        createSubscriptions(results);
    }

    public void createSubscriptions(List<Subscription> results) {
        int count = 0;
        long start = System.currentTimeMillis();
        int log = CHECK_INTERVAL;
        CTrie tree = new CTrie();
        for (Subscription result : results) {
            tree.addToTree(result);
            count++;
            log--;
            if (log <= 0) {
                log = CHECK_INTERVAL;
                long end = System.currentTimeMillis();
                long duration = end - start;
                LOGGER.info("Added {} subscriptions in {} ms ({}/ms)", count, duration, Math.round(1.0 * count / duration));
            }
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
        }
        long end = System.currentTimeMillis();
        long duration = end - start;
        LOGGER.info("Added " + count + " subscriptions in " + duration + " ms (" + Math.round(1000.0 * count / duration) + "/s)");
    }

    public List<Subscription> prepareSubscriptionsManyClientsFewTopic() {
        List<Subscription> subscriptionList = new ArrayList<>(TOTAL_SUBSCRIPTIONS);
        for (int i = 0; i < TOTAL_SUBSCRIPTIONS; i++) {
            Topic topic = asTopic("topic/test/" + new Random().nextInt(1 + i % 10) + "/test");
            subscriptionList.add(new Subscription("TestClient-" + i, topic, MqttQoS.AT_LEAST_ONCE));
        }
        return subscriptionList;
    }

    public List<Subscription> prepareSubscriptionsFlat() {
        List<Subscription> results = new ArrayList<>(TOTAL_SUBSCRIPTIONS);
        int count = 0;
        long start = System.currentTimeMillis();
        for (int topicNr = 0; topicNr < TOTAL_SUBSCRIPTIONS / 10; topicNr++) {
            for (int clientNr = 0; clientNr < 10; clientNr++) {
                count++;
                results.add(clientSubOnTopic("Client-" + clientNr, "mainTopic-" + topicNr));
            }
        }
        long end = System.currentTimeMillis();
        long duration = end - start;
        LOGGER.info("Prepared {} subscriptions in {} ms", count, duration);
        return results;
    }

    public List<Subscription> prepareSubscriptionsDeep() {
        List<Subscription> results = new ArrayList<>(TOTAL_SUBSCRIPTIONS);
        long countPerLevel = Math.round(Math.pow(TOTAL_SUBSCRIPTIONS, 0.25));
        LOGGER.info("Preparing {} subscriptions, 4 deep with {} per level", TOTAL_SUBSCRIPTIONS, countPerLevel);
        int count = 0;
        long start = System.currentTimeMillis();
        outerloop:
        for (int clientNr = 0; clientNr < countPerLevel; clientNr++) {
            for (int firstLevelNr = 0; firstLevelNr < countPerLevel; firstLevelNr++) {
                for (int secondLevelNr = 0; secondLevelNr < countPerLevel; secondLevelNr++) {
                    for (int thirdLevelNr = 0; thirdLevelNr < countPerLevel; thirdLevelNr++) {
                        count++;
                        results.add(clientSubOnTopic("Client-" + clientNr, "mainTopic-" + firstLevelNr + "/subTopic-" + secondLevelNr + "/subSubTopic" + thirdLevelNr));
                        // Due to the 4th-power-root we don't get exactly the required number of subs.
                        if (count >= TOTAL_SUBSCRIPTIONS) {
                            break outerloop;
                        }
                    }
                }
            }
        }
        long end = System.currentTimeMillis();
        long duration = end - start;
        LOGGER.info("Prepared {} subscriptions in {} ms", count, duration);
        return results;
    }

}
