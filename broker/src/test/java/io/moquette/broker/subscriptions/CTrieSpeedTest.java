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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CTrieSpeedTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CTrieSpeedTest.class.getName());

    private static final int MAX_DURATION_S = 50 * 60;
    private static final int CHECK_INTERVAL = 5000_000;
    private static final int TOTAL_SUBSCRIPTIONS = 1_000_000;

    private static final Map<Integer, Map<Integer, List<TestResult>>> TEST_RESULTS_ADD = new TreeMap<>();
    private static final Map<Integer, Map<Integer, List<TestResult>>> TEST_RESULTS_READ = new TreeMap<>();
    private static final Map<Integer, Map<Integer, List<TestResult>>> TEST_RESULTS_REMOVE = new TreeMap<>();

    private static void addAddResult(TestResult result) {
        addResult(TEST_RESULTS_ADD, result);
    }

    private static void addReadResult(TestResult result) {
        addResult(TEST_RESULTS_READ, result);
    }

    private static void addRemoveResult(TestResult result) {
        addResult(TEST_RESULTS_REMOVE, result);
    }

    private static void addResult(Map<Integer, Map<Integer, List<TestResult>>> set, TestResult result) {
        set.computeIfAbsent(result.threads, t -> new TreeMap<>())
            .computeIfAbsent(result.maxLength, t -> new ArrayList<>())
            .add(result);
    }

    private static void logResults() {
        LOGGER.info("Results for Adding:");
        logResults(TEST_RESULTS_ADD);
        LOGGER.info("Results for Reading:");
        logResults(TEST_RESULTS_READ);
        LOGGER.info("Results for Removing:");
        logResults(TEST_RESULTS_REMOVE);
    }

    private static void clearResults() {
        TEST_RESULTS_ADD.clear();
        TEST_RESULTS_READ.clear();
        TEST_RESULTS_REMOVE.clear();
    }

    private static void logResults(Map<Integer, Map<Integer, List<TestResult>>> set) {
        StringBuilder header = new StringBuilder();
        TreeMap<Integer, StringBuilder> rowPerLength = new TreeMap<>();
        for (Map.Entry<Integer, Map<Integer, List<TestResult>>> entry : set.entrySet()) {
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
                rowPerLength.computeIfAbsent(length, t -> new StringBuilder("" + t)).append(',').append(Math.round(durationAvg));
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

    @Disabled
    @Test
    public void testManyClientsFewTopics() throws InterruptedException, Exception {
        LOGGER.info("testManyClientsFewTopics");
        clearResults();
        Callable<List<SubscriptionRequest>> subCreate = () -> prepareSubscriptionsManyClientsFewTopic(20_000);
        test(subCreate);
    }

    @Disabled
    @Test
    public void testFlat() throws Exception {
        LOGGER.info("TestFlat");
        clearResults();
        Callable<List<SubscriptionRequest>> subCreate = () -> prepareSubscriptionsFlat(TOTAL_SUBSCRIPTIONS);
        test(subCreate);
    }

    @Disabled
    @Test
    public void testDeep() throws InterruptedException, Exception {
        LOGGER.info("TestDeep");
        clearResults();
        Callable<List<SubscriptionRequest>> subCreate = () -> prepareSubscriptionsDeep(TOTAL_SUBSCRIPTIONS);
        test(subCreate);
    }

    private void test(Callable<List<SubscriptionRequest>> subCreate) throws Exception {
        for (int length : new int[]{1, 2, 3, 4, 5, 6, 7, 8, 99}) {
            for (int threads : new int[]{1, 2, 4, 6, 8, 12, 16}) {
                test(subCreate, threads, length);
                test(subCreate, threads, length);
                test(subCreate, threads, length);
                logResults();
            }
        }
        logResults();
    }

    private void test(Callable<List<SubscriptionRequest>> subCreate, int threadCount, int maxLength) throws Exception {
        Topic.MAX_TOKEN_LENGTH = maxLength;
        final List<SubscriptionRequest> subRequests = subCreate.call();
        CTrie tree = createSubscriptions(subRequests, threadCount);
        readSubscriptions(tree, subRequests, threadCount);
        removeSubscriptions(tree, subRequests, threadCount);
    }

    public CTrie createSubscriptions(List<SubscriptionRequest> subsToAdd, int threadCount) throws InterruptedException {
        long start = System.currentTimeMillis();
        CTrie tree = new CTrie();
        List<List<SubscriptionRequest>> subLists = new ArrayList<>();
        final int total = subsToAdd.size();
        int perList = total / threadCount;
        int startIdx = 0;
        for (int idx = 0; idx < threadCount - 1; idx++) {
            int endIdx = startIdx + perList;
            subLists.add(subsToAdd.subList(startIdx, endIdx));
            startIdx = endIdx;
        }
        subLists.add(subsToAdd.subList(startIdx, total));

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
        addAddResult(new TestResult(threadCount, Topic.MAX_TOKEN_LENGTH, total, duration));
        return tree;
    }

    public CTrie removeSubscriptions(CTrie tree, List<SubscriptionRequest> subsToAdd, int threadCount) throws InterruptedException {
        long start = System.currentTimeMillis();

        List<List<SubscriptionRequest>> subLists = new ArrayList<>();
        final int total = subsToAdd.size();
        int perList = total / threadCount;
        int startIdx = 0;
        for (int idx = 0; idx < threadCount - 1; idx++) {
            int endIdx = startIdx + perList;
            subLists.add(subsToAdd.subList(startIdx, endIdx));
            startIdx = endIdx;
        }
        subLists.add(subsToAdd.subList(startIdx, total));

        List<Thread> threads = new ArrayList<>();
        for (int idx = 0; idx < threadCount; idx++) {
            final List<SubscriptionRequest> workList = subLists.get(idx);
            threads.add(new Thread(() -> {
                int log = CHECK_INTERVAL;
                int count = 0;
                for (SubscriptionRequest subReq : workList) {
                    tree.removeFromTree(CTrie.UnsubscribeRequest.buildNonShared(subReq.getClientId(), subReq.getTopicFilter()));
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
        addRemoveResult(new TestResult(threadCount, Topic.MAX_TOKEN_LENGTH, total, duration));
        return tree;
    }

    public void readSubscriptions(CTrie tree, List<SubscriptionRequest> subsToRead, int threadCount) throws InterruptedException {
        final long start1 = System.currentTimeMillis();
        List<List<SubscriptionRequest>> subLists = new ArrayList<>();
        final int total = subsToRead.size();
        int perList = total / threadCount;
        int startIdx = 0;
        for (int idx = 0; idx < threadCount - 1; idx++) {
            int endIdx = startIdx + perList;
            subLists.add(subsToRead.subList(startIdx, endIdx));
            startIdx = endIdx;
        }
        subLists.add(subsToRead.subList(startIdx, total));

        List<Thread> threads = new ArrayList<>();
        for (int idx = 0; idx < threadCount; idx++) {
            final List<SubscriptionRequest> workList = subLists.get(idx);
            threads.add(new Thread(() -> {
                int log = CHECK_INTERVAL;
                int count = 0;
                for (SubscriptionRequest subReq : workList) {

                    final Subscription subscription = subReq.subscription();
                    final Topic topic = subReq.getTopicFilter();
                    final List<Subscription> recursiveMatch = tree.recursiveMatch(topic);
                    final boolean contains = recursiveMatch.contains(subscription);
                    Assertions.assertTrue(contains, () -> "Failed to find " + subscription + " on " + topic + " found " + recursiveMatch.size() + " matches.");

                    count++;
                    log--;
                    if (log <= 0) {
                        log = CHECK_INTERVAL;
                        long end = System.currentTimeMillis();
                        long duration = end - start1;
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
        long duration = end - start1;
        final long speed = Math.round(1000.0 * total / duration);
        addReadResult(new TestResult(threadCount, Topic.MAX_TOKEN_LENGTH, total, duration));
        LOGGER.info("{}, {}, {}, {}, {}", threadCount, Topic.MAX_TOKEN_LENGTH, total, duration, speed);
    }

    public List<SubscriptionRequest> prepareSubscriptionsManyClientsFewTopic(int subCount) {
        List<SubscriptionRequest> subscriptionList = new ArrayList<>(subCount);
        int total = 0;
        long start = System.currentTimeMillis();
        int groupSize = subCount / 10;
        for (int i = 0; i < groupSize; i++) {
            for (int group = 0; group < 10; group++) {
                int idx = group * groupSize + i;
                Topic topic = asTopic("topic/test");
                subscriptionList.add(SubscriptionRequest.buildNonShared("TestClient-" + idx, topic, MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_LEAST_ONCE)));
                total++;
            }
        }
        long end = System.currentTimeMillis();
        long duration = end - start;
        LOGGER.info("Prepared {} subscriptions in {} ms on 10 topics", total, duration);
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
