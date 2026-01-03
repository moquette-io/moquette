/*
 * Copyright (c) 2012-2025 The original author or authors
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
package io.moquette.metrics;

import io.moquette.broker.config.IConfig;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A metrics provider used for testing.
 */
public class MetricsProviderMock extends AbstractMetricsProvider {

    private int queueCount;
    private int queueSize;

    private AtomicInteger[] sessionQueueFill;
    private int[] sessionQueueFillMax;
    private AtomicInteger[] sessionQueueOverruns;
    private int sessionCount;
    private int publishCount;
    private int[][] messageCount;

    @Override
    public void init(IConfig config) {
    }

    @Override
    public void initSessionQueues(int queueCount, int queueSize) {
        this.queueCount = queueCount;
        this.queueSize = queueSize;
        sessionQueueFill = new AtomicInteger[queueCount];
        sessionQueueFillMax = new int[queueCount];
        sessionQueueOverruns = new AtomicInteger[queueCount];
        messageCount = new int[queueCount][3];
        for (int idx = 0; idx < queueCount; idx++) {
            sessionQueueFill[idx] = new AtomicInteger();
            sessionQueueOverruns[idx] = new AtomicInteger();
        }
    }

    @Override
    public void stop() {
    }

    @Override
    public void sessionQueueInc(int queue) {
        int size = sessionQueueFill[queue].incrementAndGet();
        if (size > sessionQueueFillMax[queue]) {
            sessionQueueFillMax[queue] = size;
        }
    }

    @Override
    public void sessionQueueDec(int queue) {
        sessionQueueFill[queue].decrementAndGet();
    }

    @Override
    public void addSessionQueueOverrun(int queue) {
        sessionQueueOverruns[queue].incrementAndGet();
    }

    @Override
    public void addOpenSession() {
        sessionCount++;
    }

    @Override
    public void removeOpenSession() {
        sessionCount--;
    }

    @Override
    public void addPublish() {
        publishCount++;
    }

    @Override
    public void addMessage(int queue, int qos) {
        getMessageCount()[queue][qos]++;
    }

    /**
     * @return the queueCount
     */
    public int getQueueCount() {
        return queueCount;
    }

    /**
     * @return the queueSize
     */
    public int getQueueSize() {
        return queueSize;
    }

    /**
     * @return the sessionCount
     */
    public int getSessionCount() {
        return sessionCount;
    }

    /**
     * @return the publishCount
     */
    public int getPublishCount() {
        return publishCount;
    }

    /**
     * @return the messageCount
     */
    public int[][] getMessageCount() {
        return messageCount;
    }

    public int getQueueSizeSum() {
        int sum = 0;
        for (AtomicInteger size : sessionQueueFill) {
            sum += size.get();
        }
        return sum;
    }

    public int getSessionQueueOverrunSum() {
        int sum = 0;
        for (AtomicInteger size : sessionQueueOverruns) {
            sum += size.get();
        }
        return sum;
    }

    public int getMessageSum() {
        int sum = 0;
        for (int[] counts : messageCount) {
            for (int count : counts) {
                sum += count;
            }
        }
        return sum;
    }

    public int getMessageSum(int qos) {
        int sum = 0;
        for (int[] counts : messageCount) {
            sum += counts[qos];
        }
        return sum;
    }

    public int getSessionQueueFillSum() {
        int sum = 0;
        for (AtomicInteger size : sessionQueueFill) {
            sum += size.get();
        }
        return sum;
    }

    public int getSessionQueueFillMax() {
        int max = 0;
        for (int val : sessionQueueFillMax) {
            if (val > max) {
                max = val;
            }
        }
        return max;
    }

    public void clearSessionQueueFillMax() {
        Arrays.fill(sessionQueueFillMax, 0);
    }

    public int getSessionQueueFillMax(int queue) {
        return sessionQueueFillMax[queue];
    }

}
