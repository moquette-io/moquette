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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CTrieSpeedTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CTrieSpeedTest.class.getName());

    private static final int MAX_DURATION_S = 50 * 60;
    private static final int CHECK_INTERVAL = 5000_000;
    private static final int TOTAL_SUBSCRIPTIONS = 500_000;

    private static final Map<Integer, Map<Integer, List<TestResult>>> TEST_RESULTS = new TreeMap<>();

    private static void addResult(TestResult result) {
        TEST_RESULTS.computeIfAbsent(result.threads, t -> new TreeMap<>())
            .computeIfAbsent(result.maxLength, t -> new ArrayList<>())
            .add(result);
    }

    private static void logResults() {
        StringBuilder header = new StringBuilder();
        TreeMap<Integer, StringBuilder> rowPerLength = new TreeMap<>();
        for (Map.Entry<Integer, Map<Integer, List<TestResult>>> entry : TEST_RESULTS.entrySet()) {
            Integer threads = entry.getKey();
            Map<Integer, List<TestResult>> lengthMap = entry.getValue();
            header.append(',').append(threads);
            for (Map.Entry<Integer, List<TestResult>> innerEntry : lengthMap.entrySet()) {
                Integer length = innerEntry.getKey();
                List<TestResult> results = innerEntry.getValue();
                int count = results.size();
                double durationAvg = 0;
                for (TestResult result : results) {
                    durationAvg += 1.0 * result.durationMs / count;
                }
                rowPerLength.computeIfAbsent(length, t -> new StringBuilder("" + t)).append(',').append(durationAvg);
            }
        }
        LOGGER.info(header.toString());
        for (StringBuilder row : rowPerLength.values()) {
            LOGGER.info("{}", row);
        }
    }

    static SubscriptionRequest clientSubOnTopic(String clientID, String topicName) {
        return SubscriptionRequest.buildNonShared(clientID, asTopic(topicName), MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE));
    }

    @Test
    @Timeout(value = MAX_DURATION_S)
    public void testManyClientsFewTopics() throws InterruptedException {
        LOGGER.info("testManyClientsFewTopics");
        List<SubscriptionRequest> subscriptionList = prepareSubscriptionsManyClientsFewTopic(50_000);
        createSubscriptions(subscriptionList, 1);
    }

    @Test
    @Disabled
    public void testFlat() throws Exception {
        LOGGER.info("TestFlat");
        TEST_RESULTS.clear();
        Callable<List<SubscriptionRequest>> subCreate = () -> prepareSubscriptionsFlat(TOTAL_SUBSCRIPTIONS);
        for (int length : new int[]{1, 2, 3, 4, 5, 6}) {
            for (int threads : new int[]{1, 2, 4, 6, 8, 12, 16}) {
                test(subCreate, threads, length);
                logResults();
            }
        }
        logResults();
    }

    private void test(Callable<List<SubscriptionRequest>> subCreate, int threadCount, int maxLength) throws Exception {
        Topic.MAX_TOKEN_LENGTH = maxLength;
        createSubscriptions(subCreate.call(), threadCount);
        createSubscriptions(subCreate.call(), threadCount);
    }

    @Test
    @Timeout(value = MAX_DURATION_S)
    public void testDeep() throws InterruptedException {
        LOGGER.info("TestDeep");
        List<SubscriptionRequest> results = prepareSubscriptionsDeep(TOTAL_SUBSCRIPTIONS);
        createSubscriptions(results, 1);
    }

    public void createSubscriptions(List<SubscriptionRequest> results, int threadCount) throws InterruptedException {
        long start = System.currentTimeMillis();
        CTrie tree = new CTrie();
        List<List<SubscriptionRequest>> subLists = new ArrayList<>();
        final int total = results.size();
        int perList = total / threadCount;
        int startIdx = 0;
        for (int idx = 0; idx < threadCount - 1; idx++) {
            int endIdx = startIdx + perList;
            subLists.add(results.subList(startIdx, endIdx));
            startIdx = endIdx;
        }
        subLists.add(results.subList(startIdx, total - 1));

        List<Thread> threads = new ArrayList<>();
        for (int idx = 0; idx < threadCount; idx++) {
            final List<SubscriptionRequest> workList = subLists.get(idx);
            threads.add(new Thread(() -> {
                int log = CHECK_INTERVAL;
                int count = 0;
                for (SubscriptionRequest result : workList) {
                    tree.addToTree(result);
                    count++;
                    log--;
                    if (log <= 0) {
                        log = CHECK_INTERVAL;
                        long end = System.currentTimeMillis();
                        long duration = end - start;
                        LOGGER.info("Threads {}, Subs {}, time(ms) {}, perMs {}", threadCount, count, duration, Math.round(1.0 * count / duration));
                    }
                    if (Thread.currentThread().isInterrupted()) {
                        return;
                    }
                }
            }));
        }
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        long end = System.currentTimeMillis();
        long duration = end - start;
        final long speed = Math.round(1000.0 * total / duration);
        LOGGER.info("{}, {}, {}, {}, {}", threadCount, Topic.MAX_TOKEN_LENGTH, total, duration, speed);
        addResult(new TestResult(threadCount, Topic.MAX_TOKEN_LENGTH, total, duration));
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

    private static class TestResult {

        public final int threads;
        public final int maxLength;
        public final int topicCount;
        public final long durationMs;

        public TestResult(int threads, int maxLength, int topicCount, long durationMs) {
            this.threads = threads;
            this.maxLength = maxLength;
            this.topicCount = topicCount;
            this.durationMs = durationMs;
        }

    }
}
