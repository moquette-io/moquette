/*
 * Copyright (c) 2012-2026 The original author or authors
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

package io.moquette.broker;

import io.moquette.metrics.MetricsProviderNull;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class SessionEventLoopTest {

    @Test
    public void givenACommandThatThrowsWhenProcessedThenTheSharedSessionLoopKeepsRunning() {
        final BlockingQueue<FutureTask<String>> queue = new LinkedBlockingQueue<>();
        final SessionEventLoop sut = new SessionEventLoop(queue, 0, new MetricsProviderNull());
        sut.setDaemon(true);
        sut.start();
        try {
            // A command that throws (as a malformed packet's handler could).
            queue.add(new FutureTask<>(() -> {
                throw new RuntimeException("boom");
            }));

            // A subsequent command bound to the SAME loop must still be executed.
            final AtomicBoolean executed = new AtomicBoolean(false);
            queue.add(new FutureTask<>(() -> {
                executed.set(true);
                return "ok";
            }));

            Awaitility.await("the session loop keeps processing commands after one of them throws")
                .atMost(Duration.ofSeconds(5))
                .untilTrue(executed);
        } finally {
            sut.interrupt();
            try {
                sut.join(TimeUnit.SECONDS.toMillis(2));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
