/*
 * Copyright (c) 2012-2017 The original author or authors
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

package io.moquette.interception.messages;

import io.moquette.spi.impl.subscriptions.Topic;

public class InterceptUnsubscribeMessage implements InterceptMessage {

    private final Topic topic;
    private final String clientID;
    private final String username;

    public InterceptUnsubscribeMessage(Topic topic, String clientID, String username) {
        this.topic = topic;
        this.clientID = clientID;
        this.username = username;
    }

    public Topic getTopic() {
        return topic;
    }

    public String getClientID() {
        return clientID;
    }

    public String getUsername() {
        return username;
    }
}
