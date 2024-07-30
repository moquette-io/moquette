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

import io.moquette.broker.subscriptions.CTrie.SubscriptionRequest;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import io.netty.handler.codec.mqtt.MqttSubscriptionOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CTrieSpeedTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CTrieSpeedTest.class.getName());

    private static final int MAX_DURATION_S = 5 * 60;
    private static final int CHECK_INTERVAL = 50_000;
    private static final int TOTAL_SUBSCRIPTIONS = 500_000;

    static SubscriptionRequest clientSubOnTopic(String clientID, String topicName) {
        return SubscriptionRequest.buildNonShared(clientID, asTopic(topicName), MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE));
    }

    @Test
    @Timeout(value = MAX_DURATION_S)
    public void testManyClientsFewTopics() {

        List<SubscriptionRequest> subscriptionList = prepareSubscriptionsManyClientsFewTopic(50_000);
        createSubscriptions(subscriptionList);
    }

    @Test
    @Timeout(value = MAX_DURATION_S)
    public void testFlat() {
        Topic.MAX_TOKEN_LENGTH = 1;
        createSubscriptions(prepareSubscriptionsFlat(TOTAL_SUBSCRIPTIONS));
        Topic.MAX_TOKEN_LENGTH = 2;
        createSubscriptions(prepareSubscriptionsFlat(TOTAL_SUBSCRIPTIONS));
        Topic.MAX_TOKEN_LENGTH = 3;
        createSubscriptions(prepareSubscriptionsFlat(TOTAL_SUBSCRIPTIONS));
        Topic.MAX_TOKEN_LENGTH = 4;
        createSubscriptions(prepareSubscriptionsFlat(TOTAL_SUBSCRIPTIONS));
        Topic.MAX_TOKEN_LENGTH = 5;
        createSubscriptions(prepareSubscriptionsFlat(TOTAL_SUBSCRIPTIONS));
        Topic.MAX_TOKEN_LENGTH = 6;
        createSubscriptions(prepareSubscriptionsFlat(TOTAL_SUBSCRIPTIONS));
        Topic.MAX_TOKEN_LENGTH = 7;
        createSubscriptions(prepareSubscriptionsFlat(TOTAL_SUBSCRIPTIONS));
    }

    @Test
    @Timeout(value = MAX_DURATION_S)
    public void testDeep() {
        List<SubscriptionRequest> results = prepareSubscriptionsDeep(TOTAL_SUBSCRIPTIONS);
        createSubscriptions(results);
    }

    public void createSubscriptions(List<SubscriptionRequest> results) {
        int count = 0;
        long start = System.currentTimeMillis();
        int log = CHECK_INTERVAL;
        CTrie tree = new CTrie();
        for (SubscriptionRequest result : results) {
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
        final long speed = Math.round(1000.0 * count / duration);
        LOGGER.info("{}: Added {} subscriptions in {} ms ({}/s)", Topic.MAX_TOKEN_LENGTH, count, duration, speed);
    }

    public List<SubscriptionRequest> prepareSubscriptionsManyClientsFewTopic(int subCount) {
        List<SubscriptionRequest> subscriptionList = new ArrayList<>(subCount);
        long start = System.currentTimeMillis();
        for (int i = 0; i < subCount; i++) {
            Topic topic = asTopic("topic/test/" + new Random().nextInt(10) + "/test");
            subscriptionList.add(SubscriptionRequest.buildNonShared("TestClient-" + i, topic, MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_LEAST_ONCE)));
        }
        long end = System.currentTimeMillis();
        long duration = end - start;
        LOGGER.debug("Prepared {} subscriptions in {} ms on 10 topics", subCount, duration);
        return subscriptionList;
    }

    public List<SubscriptionRequest> prepareSubscriptionsFlat(int subCount) {
        List<SubscriptionRequest> results = new ArrayList<>(subCount);
        int count = 0;
        long start = System.currentTimeMillis();
        final int clientCount = 1;
        final int topicCount = subCount / clientCount;
        for (int clientNr = 0; clientNr < clientCount; clientNr++) {
            for (int topicNr = 0; topicNr < topicCount; topicNr++) {
                count++;
                results.add(clientSubOnTopic("Client-" + clientNr, topicNr + "-mainTopic"));
            }
        }
        long end = System.currentTimeMillis();
        long duration = end - start;
        LOGGER.debug("Prepared {} subscriptions for {} topics in {} ms", count, topicCount, duration);
        return results;
    }

    public List<SubscriptionRequest> prepareSubscriptionsDeep(int subCount) {
        List<SubscriptionRequest> results = new ArrayList<>(subCount);
        long countPerLevel = Math.round(Math.pow(subCount, 0.25));
        LOGGER.info("Preparing {} subscriptions, 4 deep with {} per level", subCount, countPerLevel);
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
                        if (count >= subCount) {
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
