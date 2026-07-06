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

package io.moquette.broker;

import io.moquette.metrics.MetricsProviderNull;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionEventLoopTest {

    @Test
    public void aCommandThatThrowsMustNotTerminateTheSharedSessionLoop() throws InterruptedException {
        final BlockingQueue<FutureTask<String>> queue = new LinkedBlockingQueue<>();
        final SessionEventLoop loop = new SessionEventLoop(queue, 0, new MetricsProviderNull());
        loop.setDaemon(true);
        loop.start();
        try {
            // A command that throws (as a malformed packet's handler could).
            queue.add(new FutureTask<>(() -> {
                throw new RuntimeException("boom");
            }));

            // A subsequent command bound to the SAME loop must still be executed.
            final CountDownLatch executed = new CountDownLatch(1);
            queue.add(new FutureTask<>(() -> {
                executed.countDown();
                return "ok";
            }));

            assertTrue(executed.await(5, TimeUnit.SECONDS),
                "the session loop must keep processing commands after one of them throws");
        } finally {
            loop.interrupt();
            loop.join(TimeUnit.SECONDS.toMillis(2));
        }
    }
}
