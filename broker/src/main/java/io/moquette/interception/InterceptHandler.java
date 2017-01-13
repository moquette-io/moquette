/*
 * Copyright (c) 2012-2017 The original author or authorsgetRockQuestions()
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

import io.moquette.interception.messages.InterceptAcknowledgedMessage;
import io.moquette.interception.messages.InterceptConnectMessage;
import io.moquette.interception.messages.InterceptConnectionLostMessage;
import io.moquette.interception.messages.InterceptDisconnectMessage;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.interception.messages.InterceptSubscribeMessage;
import io.moquette.interception.messages.InterceptUnsubscribeMessage;
import io.moquette.parser.proto.messages.AbstractMessage;
import io.moquette.spi.impl.subscriptions.Subscription;

/**
 * This interface is used to inject code for intercepting broker events.
 * <p>
 * The events can act only as observers.
 * <p>
 * Almost every method receives a subclass of {@link AbstractMessage}, except
 * <code>onDisconnect</code> that receives the client id string and
 * <code>onSubscribe</code> and <code>onUnsubscribe</code> that receive a
 * {@link Subscription} object.
 *
 * @author Wagner Macedo
 */
public interface InterceptHandler {
	
	public static final Class<?> ALL_MESSAGE_TYPES[] = { InterceptConnectMessage.class,
			InterceptDisconnectMessage.class, InterceptConnectionLostMessage.class, InterceptPublishMessage.class,
			InterceptSubscribeMessage.class, InterceptUnsubscribeMessage.class, InterceptAcknowledgedMessage.class };
	
	/**
	 * Returns the identifier of this intercept handler.
	 * @return
	 */
	String getID();
	
	/**
	 * Returns the InterceptMessage subtypes that this handler can process. If the result is null or equal to ALL_MESSAGE_TYPES,
	 * all the message types will be processed.
	 * @return
	 */
	Class<?>[] getInterceptedMessageTypes();
	
    void onConnect(InterceptConnectMessage msg);

    void onDisconnect(InterceptDisconnectMessage msg);

    void onConnectionLost(InterceptConnectionLostMessage msg);

    void onPublish(InterceptPublishMessage msg);

    void onSubscribe(InterceptSubscribeMessage msg);

    void onUnsubscribe(InterceptUnsubscribeMessage msg);
    
    void onMessageAcknowledged(InterceptAcknowledgedMessage msg);
}
