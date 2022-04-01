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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The results of routing a publish message to all clients.
 */
public class RoutingResults {

    final List<String> successedRoutings;
    final List<String> failedRoutings;
    private final CompletableFuture<Void> mergedAction;

    public RoutingResults(List<String> successedRoutings, List<String> failedRoutings, CompletableFuture<Void> mergedAction) {
        this.successedRoutings = successedRoutings;
        this.failedRoutings = failedRoutings;
        this.mergedAction = mergedAction;
    }

    public boolean isAllSuccess() {
        return failedRoutings.isEmpty();
    }

    public boolean isAllFailed() {
        return successedRoutings.isEmpty() && !failedRoutings.isEmpty();
    }

    public CompletableFuture<Void> completableFuture() {
        return mergedAction;
    }

    public static RoutingResults preroutingError() {
        // WARN this is a special case failed is empty, but this result is to be considered as error.
        return new RoutingResults(Collections.emptyList(), Collections.emptyList(), CompletableFuture.completedFuture(null));
    }

    @Override
    public String toString() {
        return "RoutingResults{" + "successedRoutings=" + successedRoutings + ", failedRoutings=" + failedRoutings + ", mergedAction=" + mergedAction + '}';
    }

}
