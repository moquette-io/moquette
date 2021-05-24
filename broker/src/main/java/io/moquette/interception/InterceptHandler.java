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

package io.moquette.interception;

import io.moquette.interception.messages.*;
import io.moquette.broker.subscriptions.Subscription;
import io.netty.handler.codec.mqtt.MqttMessage;

/**
 * This interface is used to inject code for intercepting broker events.
 * <p>
 * The events can act only as observers.
 * <p>
 * Almost every method receives a subclass of {@link MqttMessage}, except <code>onDisconnect</code>
 * that receives the client id string and <code>onSubscribe</code> and <code>onUnsubscribe</code>
 * that receive a {@link Subscription} object.
 */
public interface InterceptHandler {

    Class<?>[] ALL_MESSAGE_TYPES = {InterceptConnectMessage.class, InterceptDisconnectMessage.class,
            InterceptConnectionLostMessage.class, InterceptPublishMessage.class, InterceptSubscribeMessage.class,
            InterceptUnsubscribeMessage.class, InterceptAcknowledgedMessage.class};

    /**
     * @return the identifier of this intercept handler.
     */
    String getID();

    /**
     * @return the InterceptMessage subtypes that this handler can process. If the result is null or
     * equal to ALL_MESSAGE_TYPES, all the message types will be processed.
     */
    Class<?>[] getInterceptedMessageTypes();

    void onConnect(InterceptConnectMessage msg);

    void onDisconnect(InterceptDisconnectMessage msg);

    void onConnectionLost(InterceptConnectionLostMessage msg);

    /**
     * Called when a message is published. The receiver MUST release the payload of the message, either
     * by calling super.onPublish, or by calling msg.getPayload.release() directly.
     *
     * @param msg The message that was published.
     */
    void onPublish(InterceptPublishMessage msg);

    void onSubscribe(InterceptSubscribeMessage msg);

    void onUnsubscribe(InterceptUnsubscribeMessage msg);

    void onMessageAcknowledged(InterceptAcknowledgedMessage msg);
}
