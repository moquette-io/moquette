/*
 * Copyright (c) 2012-2023 The original author or authors
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
package io.moquette.broker.subscriptions;

import io.moquette.broker.ISubscriptionsRepository;
import io.moquette.persistence.MemorySubscriptionsRepository;
import org.junit.jupiter.api.BeforeEach;

abstract class CTrieSubscriptionDirectMatchingCommon {

    protected CTrieSubscriptionDirectory sut;
    protected ISubscriptionsRepository sessionsRepository;

    @BeforeEach
    public void setUp() {
        sut = new CTrieSubscriptionDirectory();

        this.sessionsRepository = new MemorySubscriptionsRepository();
        sut.init(this.sessionsRepository);
    }
}
