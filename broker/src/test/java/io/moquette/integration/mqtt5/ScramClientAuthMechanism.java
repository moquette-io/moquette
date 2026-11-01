/*
 *
 *  * Copyright (c) 2012-2024 The original author or authors
 *  * ------------------------------------------------------
 *  * All rights reserved. This program and the accompanying materials
 *  * are made available under the terms of the Eclipse Public License v1.0
 *  * and Apache License v2.0 which accompanies this distribution.
 *  *
 *  * The Eclipse Public License is available at
 *  * http://www.eclipse.org/legal/epl-v10.html
 *  *
 *  * The Apache License v2.0 is available at
 *  * http://www.opensource.org/licenses/apache2.0.php
 *  *
 *  * You may elect to redistribute this code under either of these licenses.
 *
 */

package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.datatypes.MqttUtf8String;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientConfig;
import com.hivemq.client.mqtt.mqtt5.auth.Mqtt5EnhancedAuthMechanism;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5Auth;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5AuthBuilder;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5EnhancedAuthBuilder;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.disconnect.Mqtt5Disconnect;
import com.ongres.scram.client.ScramClient;
import com.ongres.scram.common.exception.ScramException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Adapts the SCRAM client of com.ongres.scram:scram-client to the HiveMQ client enhanced authentication.
 * <p>
 * Flow: the client-first-message is sent in the CONNECT, the server-first-message is answered with the
 * client-final-message, and the server-final-message is verified by the ScramClient (server signature) and
 * acknowledged with an empty AUTH.
 * An instance handles one authentication, so it must not be shared between clients.
 * */
class ScramClientAuthMechanism implements Mqtt5EnhancedAuthMechanism {

    private static final String MECHANISM_NAME = "SCRAM-SHA-256";

    private enum State { INITIAL, CLIENT_FIRST_SENT, CLIENT_FINAL_SENT, SERVER_VERIFIED }

    private final ScramClient scramClient;
    private State state = State.INITIAL;

    ScramClientAuthMechanism(String username, String password) {
        this.scramClient = ScramClient.builder()
            .advertisedMechanisms(Collections.singletonList(MECHANISM_NAME))
            .username(username)
            .password(password.toCharArray())
            .build();
    }

    @Override
    public MqttUtf8String getMethod() {
        return MqttUtf8String.of(MECHANISM_NAME);
    }

    @Override
    public int getTimeout() {
        return 10; // seconds
    }

    @Override
    public CompletableFuture<Void> onAuth(Mqtt5ClientConfig config, Mqtt5Connect connect, Mqtt5EnhancedAuthBuilder authBuilder) {
        authBuilder.data(bytes(scramClient.clientFirstMessage().toString()));
        state = State.CLIENT_FIRST_SENT;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Boolean> onContinue(Mqtt5ClientConfig config, Mqtt5Auth auth, Mqtt5AuthBuilder authBuilder) {
        try {
            switch (state) {
                case CLIENT_FIRST_SENT:
                    scramClient.serverFirstMessage(text(auth));
                    authBuilder.data(bytes(scramClient.clientFinalMessage().toString()));
                    state = State.CLIENT_FINAL_SENT;
                    return CompletableFuture.completedFuture(true);
                case CLIENT_FINAL_SENT:
                    // throws if the server reports an error or the server signature is not valid
                    scramClient.serverFinalMessage(text(auth));
                    authBuilder.data(new byte[0]);
                    state = State.SERVER_VERIFIED;
                    return CompletableFuture.completedFuture(true);
                default:
                    return failed("Unexpected AUTH in state " + state);
            }
        } catch (ScramException e) {
            return failed(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> onAuthSuccess(Mqtt5ClientConfig config, Mqtt5ConnAck connAck) {
        // the connection is accepted only if the server has been verified
        return CompletableFuture.completedFuture(state == State.SERVER_VERIFIED);
    }

    @Override
    public CompletableFuture<Void> onReAuth(Mqtt5ClientConfig config, Mqtt5AuthBuilder authBuilder) {
        return failed("Re-authentication not supported");
    }

    @Override
    public CompletableFuture<Boolean> onServerReAuth(Mqtt5ClientConfig config, Mqtt5Auth auth, Mqtt5AuthBuilder authBuilder) {
        return failed("Re-authentication not supported");
    }

    @Override
    public CompletableFuture<Boolean> onReAuthSuccess(Mqtt5ClientConfig config, Mqtt5Auth auth) {
        return failed("Re-authentication not supported");
    }

    @Override
    public void onAuthRejected(Mqtt5ClientConfig config, Mqtt5ConnAck connAck) {
    }

    @Override
    public void onReAuthRejected(Mqtt5ClientConfig config, Mqtt5Disconnect disconnect) {
    }

    @Override
    public void onAuthError(Mqtt5ClientConfig config, Throwable cause) {
    }

    @Override
    public void onReAuthError(Mqtt5ClientConfig config, Throwable cause) {
    }

    private static <T> CompletableFuture<T> failed(String message) {
        return failed(new IllegalStateException(message));
    }

    private static <T> CompletableFuture<T> failed(Throwable cause) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(cause);
        return future;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(Mqtt5Auth auth) {
        final ByteBuffer data = auth.getData().orElseThrow(() -> new IllegalStateException("AUTH without data"));
        final byte[] content = new byte[data.remaining()];
        data.duplicate().get(content);
        return new String(content, StandardCharsets.UTF_8);
    }
}
