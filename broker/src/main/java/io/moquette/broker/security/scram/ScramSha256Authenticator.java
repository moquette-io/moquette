/*
 * Copyright (c) 2012-2026 The original author or authors
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

package io.moquette.broker.security.scram;

import com.ongres.scram.common.ClientFirstMessage;
import com.ongres.scram.common.ScramFunctions;
import com.ongres.scram.common.ScramMechanism;
import com.ongres.scram.common.ServerFinalMessage;
import com.ongres.scram.common.ServerFirstMessage;
import com.ongres.scram.common.exception.ScramParseException;
import io.moquette.broker.security.ExtendedAuthenticator;
import io.moquette.broker.security.scram.ScramCredentialsGenerator.ScramUserData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.function.Function;

/**
 * Server side of the SCRAM-SHA-256 mechanism (RFC 5802, RFC 7677), without channel binding.
 * <p>
 * An instance is stateful and handles one authentication exchange, so a new instance has to be used for
 * each connection. The exchange, driven by successive calls to {@link #processStep(byte[])}, is:
 * <ol>
 *   <li>client-first-message (CONNECT data) -> server-first-message (continue)</li>
 *   <li>client-final-message (AUTH) -> server-final-message (continue), only if the client proof is valid</li>
 *   <li>empty acknowledge from the client (AUTH) -> success</li>
 * </ol>
 * Any protocol violation or wrong proof makes the step fail, and every following step fails too.
 * */
public final class ScramSha256Authenticator implements ExtendedAuthenticator {

    private static final Logger LOG = LoggerFactory.getLogger(ScramSha256Authenticator.class);

    private static final ScramMechanism MECHANISM = ScramMechanism.SCRAM_SHA_256;
    private static final int NONCE_LENGTH = 24;
    private static final int FAKE_SALT_LENGTH = 32;
    private static final int FAKE_ITERATION_COUNT = 4096;

    private enum State {
        WAITING_CLIENT_FIRST, WAITING_CLIENT_FINAL, WAITING_CLIENT_ACK, COMPLETED, FAILED
    }

    private final Function<String, ScramUserData> credentialsLookup;
    private final SecureRandom random = new SecureRandom();

    private State state = State.WAITING_CLIENT_FIRST;

    // data kept between steps
    private String gs2Header;
    private String clientFirstBare;
    private String serverFirst;
    private String combinedNonce;
    private String username;
    // null when the user is unknown, in that case the exchange goes on with fake data and fails on the proof
    private ScramUserData userData;
    private String authenticatedUsername;

    /**
     * Authenticator that looks up users in the config/users.yaml file of the Moquette installation.
     * */
    public ScramSha256Authenticator() {
        this(ScramUserLookup.forInstallation());
    }

    /**
     * @param credentialsLookup returns the stored data of a user by username, or {@link ScramUserLookup#USER_NOT_PRESENT}
     *                          (or null) if the user is not registered.
     * */
    public ScramSha256Authenticator(Function<String, ScramUserData> credentialsLookup) {
        this.credentialsLookup = credentialsLookup;
    }

    @Override
    public String name() {
        return MECHANISM.getName();
    }

    /**
     * @return the username authenticated, available only after the exchange completed successfully.
     * */
    public String authenticatedUsername() {
        return authenticatedUsername;
    }

    @Override
    public AuthenticatorResult processStep(byte[] value) {
        try {
            switch (state) {
                case WAITING_CLIENT_FIRST:
                    return processClientFirst(value);
                case WAITING_CLIENT_FINAL:
                    return processClientFinal(value);
                case WAITING_CLIENT_ACK:
                    state = State.COMPLETED;
                    authenticatedUsername = username;
                    return AuthenticatorResult.success();
                default:
                    LOG.warn("SCRAM step received in state {}", state);
                    return fail();
            }
        } catch (ScramParseException | IllegalArgumentException e) {
            // malformed message or invalid base64
            LOG.info("Invalid SCRAM message in state {}: {}", state, e.getMessage());
            return fail();
        }
    }

    private AuthenticatorResult fail() {
        state = State.FAILED;
        return AuthenticatorResult.failure();
    }

    // client-first-message = gs2-header client-first-message-bare
    // example: n,,n=user,r=clientNonce
    private AuthenticatorResult processClientFirst(byte[] value) throws ScramParseException {
        final String message = new String(value, StandardCharsets.UTF_8);
        final ClientFirstMessage clientFirst = ClientFirstMessage.parseFrom(message);
        if (clientFirst.isChannelBindingRequired()) {
            LOG.info("SCRAM client requires channel binding, not supported");
            return fail();
        }

        // the gs2 header is made of the first two comma separated fields, including the trailing comma
        final int bareStart = message.indexOf(',', message.indexOf(',') + 1) + 1;
        gs2Header = message.substring(0, bareStart);
        clientFirstBare = message.substring(bareStart);
        username = clientFirst.getUsername();
        final ScramUserData found = credentialsLookup.apply(username);
        userData = found == ScramUserLookup.USER_NOT_PRESENT ? null : found;

        final String salt;
        final int iterationCount;
        if (userData != null) {
            salt = userData.getBase64Salt();
            iterationCount = userData.getIterationCount();
        } else {
            // don't reveal that the user doesn't exist, go on and fail later when verifying the proof
            salt = Base64.getEncoder().encodeToString(ScramFunctions.salt(FAKE_SALT_LENGTH, random));
            iterationCount = FAKE_ITERATION_COUNT;
        }

        final String serverNonce = ScramFunctions.nonce(NONCE_LENGTH, random);
        combinedNonce = clientFirst.getClientNonce() + serverNonce;
        serverFirst = new ServerFirstMessage(clientFirst.getClientNonce(), serverNonce, salt, iterationCount).toString();

        state = State.WAITING_CLIENT_FINAL;
        return AuthenticatorResult.continueFlow(serverFirst.getBytes(StandardCharsets.UTF_8));
    }

    // client-final-message = client-final-message-without-proof "," proof
    // example: c=biws,r=clientNonceServerNonce,p=base64Proof
    private AuthenticatorResult processClientFinal(byte[] value) {
        final String message = new String(value, StandardCharsets.UTF_8);
        final int proofIndex = message.lastIndexOf(",p=");
        if (proofIndex < 0) {
            LOG.info("SCRAM client-final-message without proof");
            return fail();
        }
        final String withoutProof = message.substring(0, proofIndex);
        final byte[] clientProof = Base64.getDecoder().decode(message.substring(proofIndex + ",p=".length()));

        final String[] attributes = withoutProof.split(",", -1);
        if (attributes.length != 2 || !attributes[0].startsWith("c=") || !attributes[1].startsWith("r=")) {
            LOG.info("SCRAM client-final-message with unexpected attributes");
            return fail();
        }
        // channel binding data must be the same gs2 header sent in the first message
        final String expectedCbind = Base64.getEncoder().encodeToString(gs2Header.getBytes(StandardCharsets.UTF_8));
        if (!expectedCbind.equals(attributes[0].substring(2))) {
            LOG.info("SCRAM client-final-message with channel binding not matching the client-first-message");
            return fail();
        }
        if (!combinedNonce.equals(attributes[1].substring(2))) {
            LOG.info("SCRAM client-final-message with a nonce not matching the one of the server-first-message");
            return fail();
        }
        if (userData == null) {
            LOG.info("SCRAM authentication failed for unknown user");
            return fail();
        }

        final String authMessage = clientFirstBare + "," + serverFirst + "," + withoutProof;
        final byte[] storedKey = Base64.getDecoder().decode(userData.getBase64StoredKey());
        if (!ScramFunctions.verifyClientProof(MECHANISM, clientProof, storedKey, authMessage)) {
            LOG.info("SCRAM authentication failed, invalid client proof for user {}", username);
            return fail();
        }

        // proof is valid, send the server signature so that the client can authenticate the server
        final byte[] serverKey = Base64.getDecoder().decode(userData.getBase64ServerKey());
        final byte[] serverSignature = ScramFunctions.serverSignature(MECHANISM, serverKey, authMessage);
        final String serverFinal = new ServerFinalMessage(serverSignature).toString();

        state = State.WAITING_CLIENT_ACK;
        return AuthenticatorResult.continueFlow(serverFinal.getBytes(StandardCharsets.UTF_8));
//        return AuthenticatorResult.success(serverFinal.getBytes(StandardCharsets.UTF_8));
    }
}
