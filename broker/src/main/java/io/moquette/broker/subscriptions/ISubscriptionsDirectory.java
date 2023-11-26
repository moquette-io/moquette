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

import io.moquette.broker.ISubscriptionsRepository;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.util.List;
import java.util.Set;

/**
 * Contains all topic filters that are used to  match against topic names.
 * */
public interface ISubscriptionsDirectory {

    void init(ISubscriptionsRepository sessionsRepository);

    Set<String> listAllSessionIds();

    List<Subscription> matchWithoutQosSharpening(Topic topic);

    List<Subscription> matchQosSharpening(Topic topic);

    void add(String clientId, Topic filter, MqttQoS requestedQoS);

    void removeSubscription(Topic topic, String clientID);

    int size();

    String dumpTree();
}
