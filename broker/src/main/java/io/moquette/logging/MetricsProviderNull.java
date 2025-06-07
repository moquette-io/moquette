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
 * A MetricsProvider interface that does nothing.
 */
public class MetricsProviderNull implements MetricsProvider {

    @Override
    public void init(IConfig config) {
        // Nothing to configure for this implementation.
    }

    @Override
    public void stop() {
        // Nothing to configure for this implementation.
    }

    @Override
    public void initSessionQueues(int queueCount, int queueSize) {
        // ignored
    }

    @Override
    public void sessionQueueInc(int queue) {
        // ignored
    }

    @Override
    public void sessionQueueDec(int queue) {
        // ignored
    }

    @Override
    public void addSessionQueueOverrun(int queue) {
        // ignored
    }

    @Override
    public void addOpenSession() {
        // ignored
    }

    @Override
    public void removeOpenSession() {
        // ignored
    }

    @Override
    public void addPublish() {
        // ignored
    }

    @Override
    public void addMessage(int queue, int qos) {
        // ignored
    }

}
