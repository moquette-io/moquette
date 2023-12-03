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
