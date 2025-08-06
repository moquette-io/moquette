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
import static io.moquette.broker.config.IConfig.METRICS_PROVIDER_CLASS;
import io.netty.util.internal.StringUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the metrics system.
 */
public class MetricsManager {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsManager.class);

    public static MetricsProvider createMetricsProvider(IConfig config) {
        ServiceLoader<MetricsProvider> loader = ServiceLoader.load(MetricsProvider.class);
        String classname = config.getProperty(METRICS_PROVIDER_CLASS, "");
        List<MetricsProvider> foundProviders = new ArrayList<>();
        loader.forEach(foundProviders::add);

        MetricsProvider metricsProvider = new MetricsProviderNull();

        Optional<MetricsProvider> usedProviderOpt = foundProviders.stream()
                .filter(provider -> providerMatchClassname(provider, classname))
                .findFirst();
        if (usedProviderOpt.isPresent()) {
            MetricsProvider usedProvider = usedProviderOpt.get();
            LOG.info("Using configured MetricsProvider: {}", usedProvider.getClass().getName());
            metricsProvider = usedProvider;
        } else {
            LOG.info("No MetricsProvider configured, or no matching found, using NULL provider. Available providers: {}",
                    foundProviders.stream().map(p -> p.getClass().getName()).collect(Collectors.toList()));
        }
        metricsProvider.init(config);
        return metricsProvider;
    }

    private static boolean providerMatchClassname(MetricsProvider provider, String classname) {
        return !StringUtil.isNullOrEmpty(classname) && provider.getClass().getName().endsWith(classname);
    }
}
