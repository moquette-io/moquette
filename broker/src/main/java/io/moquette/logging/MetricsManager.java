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
import static io.moquette.broker.config.IConfig.METRICS_PROVIDER_CLASS;
import io.netty.util.internal.StringUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the metrics system.
 */
public class MetricsManager {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsManager.class);

    /**
     * To ensure there is always an implementation ready, we initialise with a
     * null provider, and replace on init.
     */
    private static MetricsProvider metricsProvider = new MetricsProviderNull();

    public static void init(IConfig config) {
        ServiceLoader<MetricsProvider> loader = ServiceLoader.load(MetricsProvider.class);
        String classname = config.getProperty(METRICS_PROVIDER_CLASS, "");

        MetricsProvider usedProvider = null;
        List<String> foundProviders = new ArrayList<>();
        for (MetricsProvider provider : loader) {
            foundProviders.add(provider.getClass().getName());
            if (!StringUtil.isNullOrEmpty(classname) && provider.getClass().getName().endsWith(classname)) {
                LOG.info("Using configured MetricsProvider: {}", provider.getClass().getName());
                usedProvider = provider;
                break;
            }
        }
        if (usedProvider == null) {
            LOG.info("No MetricsProvider configured, or no matching found, using NULL provider. Available providers: {}", foundProviders);
        } else {
            metricsProvider = usedProvider;
        }
        metricsProvider.init(config);
    }

    public static void stop() {
        metricsProvider.stop();
    }

    public static MetricsProvider getMetricsProvider() {
        return metricsProvider;
    }
}
