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

/**
 * Interface that a metrics implementation must implement.
 * It mainly defines methods that are used to track Moquette metrics.
 */
public interface MetricsProvider {

    public void init(IConfig config);

    public void stop();

    /**
     * Notify the metrics provider about the number and size of session queues. This will
     * be called once.
     * @param queueCount The number of session queues.
     * @param queueSize The size of each session queue.
     */
    public void initSessionQueues(int queueCount, int queueSize);

    /**
     * Increase the fill level of the given session queue.
     * @param queue The queueId, 0-based.
     */
    public void sessionQueueInc(int queue);

    /**
     * Decrease the fill level of the given session queue.
     * @param queue The queueId, 0-based.
     */
    public void sessionQueueDec(int queue);

    /**
     * Add a queue overrun event for the given queue.
     * @param queue The queueId, 0-based.
     */
    public void addSessionQueueOverrun(int queue);

    /**
     * Register the addition of a new session.
     */
    public void addOpenSession();

    /**
     * Register the removal of a session.
     */
    public void removeOpenSession();

    /**
     * Register a publish.
     */
    public void addPublish();

    /**
     * Register the sending of a message to a client, on the given session queue.
     * @param queue The queueId, 0-based, or -1 if called from a non-session thread.
     * @param qos The QoS of the message.
     */
    public void addMessage(int queue, int qos);
}
