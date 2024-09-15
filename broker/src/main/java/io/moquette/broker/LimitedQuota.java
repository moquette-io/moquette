/*
 *
 * Copyright (c) 2012-2024 The original author or authors
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
 *
 */

package io.moquette.broker;

class LimitedQuota implements Quota {
    private int receiveMaximum;
    private int receivedQuota;

    public LimitedQuota(int receiveMaximum) {
        this.receiveMaximum = receiveMaximum;
        this.receivedQuota = receiveMaximum;
    }

    @Override
    public boolean hasLimit() {
        return true;
    }

    @Override
    public void decrement() {
        assert receivedQuota > 0;
        receivedQuota--;
    }

    @Override
    public void increment() {
        receivedQuota++;
        assert receivedQuota <= receiveMaximum;
    }

    @Override
    public boolean isConsumed() {
        return receivedQuota == 0;
    }

    @Override
    public int getMaximum() {
        return receiveMaximum;
    }

    @Override
    public String toString() {
        return "limited quota to " + receivedQuota;
    }
}
