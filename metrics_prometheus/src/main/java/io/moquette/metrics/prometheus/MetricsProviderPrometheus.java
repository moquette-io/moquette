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
package io.moquette.metrics.prometheus;

import io.moquette.broker.config.IConfig;
import io.moquette.logging.MetricsProvider;
import io.prometheus.metrics.core.datapoints.CounterDataPoint;
import io.prometheus.metrics.core.datapoints.GaugeDataPoint;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Metrics Provider that uses Prometheus.
 */
public class MetricsProviderPrometheus implements MetricsProvider {

    public static final String TAG_ENDPOINT_PORT = "metrics_endpoint_port";

    private static final Logger LOG = LoggerFactory.getLogger(MetricsProviderPrometheus.class);

    private HTTPServer metricsServer;

    private Gauge sessionQueueFillGauge;
    private GaugeDataPoint[] sessionQueueFillGauges;
    private Counter sessionQueueOverrunCounter;
    private CounterDataPoint[] sessionQueueOverrunCounters;
    private Gauge openSessionsGauge;
    private Counter messageCounter;
    private CounterDataPoint[][] messageCounters;
    private Counter publishCounter;

    @Override
    public void init(IConfig config) {
        LOG.info("Initialising Prometheus metrics provider.");
        int metricsPort = config.intProp(TAG_ENDPOINT_PORT, 9400);
        try {
            // initialize the out-of-the-box JVM metrics
            JvmMetrics.builder().register();
            if (metricsPort > 0) {
                metricsServer = HTTPServer.builder()
                        .port(metricsPort)
                        .buildAndStart();
                LOG.info("Prometheus metrics endpoint started on port {}", metricsPort);
            }
        } catch (IOException ex) {
            LOG.error("Failed to start metrics server.", ex);
        }
        openSessionsGauge = Gauge.builder()
                .name("moquette_open_sessions")
                .help("The number of open sessions in the broker.")
                .register();

        publishCounter = Counter.builder()
                .name("moquette_publishes")
                .help("Number of publishes made on the broker")
                .register();
    }

    @Override
    public void stop() {
        if (metricsServer != null) {
            metricsServer.stop();
        }
    }

    @Override
    public void initSessionQueues(int queueCount, int queueSize) {
        sessionQueueFillGauge = Gauge.builder()
                .name("moquette_session_queue_fill")
                .help("Number of items in the Session Queue")
                .labelNames("queue_id")
                .register();

        sessionQueueOverrunCounter = Counter.builder()
                .name("moquette_session_queue_overruns")
                .help("Number of items dropped because the queue was full")
                .labelNames("queue_name")
                .register();

        messageCounter = Counter.builder()
                .name("moquette_session_messages")
                .help("Number of messages send by this session queue")
                .labelNames("queue_name", "QoS")
                .register();

        sessionQueueFillGauges = new GaugeDataPoint[queueCount];
        sessionQueueOverrunCounters = new CounterDataPoint[queueCount];
        messageCounters = new CounterDataPoint[queueCount][3];
        for (int id = 0; id < queueCount; id++) {
            final String label = "queue-" + id;
            sessionQueueFillGauges[id] = sessionQueueFillGauge.labelValues(label);
            sessionQueueFillGauges[id].set(0);
            sessionQueueOverrunCounters[id] = sessionQueueOverrunCounter.labelValues(label);
            sessionQueueOverrunCounter.initLabelValues(label);
            for (int qos = 0; qos <= 2; qos++) {
                messageCounters[id][qos] = messageCounter.labelValues(label, Integer.toString(qos));
                messageCounter.initLabelValues(label, Integer.toString(qos));
            }
        }
    }

    @Override
    public void sessionQueueInc(int queue) {
        if (queue >= sessionQueueFillGauges.length) {
            return;
        }
        sessionQueueFillGauges[queue].inc();
    }

    @Override
    public void sessionQueueDec(int queue) {
        if (queue >= sessionQueueFillGauges.length) {
            return;
        }
        sessionQueueFillGauges[queue].dec();
    }

    @Override
    public void addSessionQueueOverrun(int queue) {
        if (queue >= sessionQueueOverrunCounters.length) {
            return;
        }
        sessionQueueOverrunCounters[queue].inc();
    }

    @Override
    public void addOpenSession() {
        openSessionsGauge.inc();
    }

    @Override
    public void removeOpenSession() {
        openSessionsGauge.dec();
    }

    @Override
    public void addPublish() {
        publishCounter.inc();
    }

    @Override
    public void addMessage(int queue, int qos) {
        if (queue < 0 || queue >= messageCounters.length) {
            return;
        }
        messageCounters[queue][qos].inc();
    }

}
