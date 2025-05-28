/*
 * Copyright (c) 2012-2018 The original author or authors
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
package io.moquette.logging;

import io.moquette.broker.config.IConfig;

/**
 * Interface that a metrics implementation must implement.
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
}
