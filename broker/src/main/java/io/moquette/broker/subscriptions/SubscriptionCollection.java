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
package io.moquette.broker.subscriptions;

import static io.moquette.broker.subscriptions.CNode.SECURE_RANDOM;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A wrapper over multiple maps of normal subscriptions.
 */
public class SubscriptionCollection implements Iterable<Subscription> {

    private final List<Map<String, Subscription>> normalSubscriptions = new ArrayList<>();
    private final List<Map<ShareName, List<SharedSubscription>>> sharedSubscriptions = new ArrayList<>();

    public boolean isEmpty() {
        return normalSubscriptions.isEmpty() && sharedSubscriptions.isEmpty();
    }

    /**
     * Calculates the number of subscriptions. Expensive, only use for tests!
     * @return the number of subscriptions.
     */
    public int size() {
        int total = 0;
        for (Map<String, Subscription> var : normalSubscriptions) {
            total += var.size();
        }
        for (Map<ShareName, List<SharedSubscription>> var : sharedSubscriptions) {
            total += var.size();
        }
        return total;
    }

    public void addNormalSubscriptions(Map<String, Subscription> subs) {
        if (subs.isEmpty()) {
            return;
        }
        normalSubscriptions.add(subs);
    }

    public void addSharedSubscriptions(Map<ShareName, List<SharedSubscription>> subs) {
        if (sharedSubscriptions.isEmpty()) {
            return;
        }
        sharedSubscriptions.add(subs);
    }

    private static Subscription selectRandom(List<SharedSubscription> list) {
        // select a subscription randomly
        int randIdx = SECURE_RANDOM.nextInt(list.size());
        return list.get(randIdx).createSubscription();
    }

    @Override
    public Iterator<Subscription> iterator() {
        return new IteratorImpl(this);
    }

    private static class IteratorImpl implements Iterator<Subscription> {

        private Iterator<Map<String, Subscription>> normapSubListIter;
        private Iterator<Subscription> normalSubIter;

        private Iterator<Map<ShareName, List<SharedSubscription>>> sharedSubMapIter;
        private Iterator<List<SharedSubscription>> sharedSubIter;

        public IteratorImpl(SubscriptionCollection parent) {
            normapSubListIter = parent.normalSubscriptions.iterator();
            sharedSubMapIter = parent.sharedSubscriptions.iterator();
        }

        @Override
        public boolean hasNext() {
            if (normalSubIter != null && normalSubIter.hasNext()) {
                return true;
            }
            if (sharedSubIter != null && sharedSubIter.hasNext()) {
                return true;
            }
            if (normapSubListIter != null) {
                if (normapSubListIter.hasNext()) {
                    // Get the next normal subscriptions iterator.
                    Map<String, Subscription> next = normapSubListIter.next();
                    normalSubIter = next.values().iterator();
                    return true;
                } else {
                    // Reached the end of the normal subscriptions lists.
                    normapSubListIter = null;
                }
            }
            if (sharedSubMapIter != null) {
                if (sharedSubMapIter.hasNext()) {
                    Map<ShareName, List<SharedSubscription>> next = sharedSubMapIter.next();
                    sharedSubIter = next.values().iterator();
                    return true;
                } else {
                    sharedSubMapIter = null;
                }
            }
            return false;
        }

        @Override
        public Subscription next() {
            if (normalSubIter != null) {
                return normalSubIter.next();
            }
            if (sharedSubIter != null) {
                return selectRandom(sharedSubIter.next());
            }
            throw new NoSuchElementException("Fetched past the end of Iterator, make sure to call hasNext!");
        }
    }

}
