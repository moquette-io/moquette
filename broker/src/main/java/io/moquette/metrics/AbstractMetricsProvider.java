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

/**
 * An abstract implementation of the MetricsProvider with some convenience
 * functions.
 */
public abstract class AbstractMetricsProvider implements MetricsProvider {

    private boolean queuesInitialised;

    @Override
    public void initSessionQueues(int queueCount, int queueSize) {
        if (queuesInitialised) {
            throw new IllegalStateException("initSessioQueues must be called only once and was already executed");
        }
        queuesInitialised = true;
    }

}
