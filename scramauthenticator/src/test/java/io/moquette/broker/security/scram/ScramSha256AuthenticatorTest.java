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

import com.ongres.scram.common.ScramFunctions;
import com.ongres.scram.common.ScramMechanism;
import com.ongres.scram.common.StringPreparation;
import io.moquette.broker.security.ExtendedAuthenticator.AuthenticatorResult;
import io.moquette.broker.security.scram.ScramCredentialsGenerator.ScramUserData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScramSha256AuthenticatorTest {

    private static final ScramMechanism MECH = ScramMechanism.SCRAM_SHA_256;
    private static final String GS2_HEADER = "n,,";
    private static final String CLIENT_NONCE = "rOprNGfwEbeRWgbNEkqO";

    private ScramSha256Authenticator authenticator;

    @BeforeEach
    void setUp() {
        final ScramUserData bob = ScramCredentialsGenerator.registerNewUser("bob", "secret".toCharArray(), 4096);
        authenticator = new ScramSha256Authenticator(user -> "bob".equals(user) ? bob : null);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(AuthenticatorResult result) {
        return new String(result.getReplyData(), StandardCharsets.UTF_8);
    }

    private static String attribute(String message, String name) {
        for (String attr : message.split(",")) {
            if (attr.startsWith(name + "=")) {
                return attr.substring(name.length() + 1);
            }
        }
        throw new IllegalArgumentException("missing " + name + " in " + message);
    }

    /** Builds the client-final-message like a client would do. */
    private static String clientFinal(String clientFirstBare, String serverFirst, String password) {
        final String withoutProof = "c=" + Base64.getEncoder().encodeToString(bytes(GS2_HEADER))
            + ",r=" + attribute(serverFirst, "r");
        final String authMessage = clientFirstBare + "," + serverFirst + "," + withoutProof;

        final byte[] salt = Base64.getDecoder().decode(attribute(serverFirst, "s"));
        final int iterations = Integer.parseInt(attribute(serverFirst, "i"));
        final byte[] saltedPassword = ScramFunctions.saltedPassword(MECH, StringPreparation.SASL_PREPARATION,
            password.toCharArray(), salt, iterations);
        final byte[] clientKey = ScramFunctions.clientKey(MECH, saltedPassword);
        final byte[] storedKey = ScramFunctions.storedKey(MECH, clientKey);
        final byte[] clientSignature = ScramFunctions.clientSignature(MECH, storedKey, authMessage);
        final byte[] proof = ScramFunctions.clientProof(clientKey, clientSignature);
        return withoutProof + ",p=" + Base64.getEncoder().encodeToString(proof);
    }

    @Test
    void completeSuccessfulExchange() {
        final String clientFirstBare = "n=bob,r=" + CLIENT_NONCE;

        AuthenticatorResult first = authenticator.processStep(bytes(GS2_HEADER + clientFirstBare));
        assertFalse(first.isFailure());
        assertFalse(first.isCompleted());
        final String serverFirst = text(first);
        assertTrue(attribute(serverFirst, "r").startsWith(CLIENT_NONCE));
        assertEquals("4096", attribute(serverFirst, "i"));

        AuthenticatorResult second = authenticator.processStep(bytes(clientFinal(clientFirstBare, serverFirst, "secret")));
        assertFalse(second.isFailure());
        assertFalse(second.isCompleted());
        final String serverFinal = text(second);

        // client verifies the server signature
        final String withoutProof = "c=" + Base64.getEncoder().encodeToString(bytes(GS2_HEADER)) + ",r=" + attribute(serverFirst, "r");
        final String authMessage = clientFirstBare + "," + serverFirst + "," + withoutProof;
        final byte[] salt = Base64.getDecoder().decode(attribute(serverFirst, "s"));
        final byte[] serverKey = ScramFunctions.serverKey(MECH, ScramFunctions.saltedPassword(MECH,
            StringPreparation.SASL_PREPARATION, "secret".toCharArray(), salt, 4096));
        assertArrayEquals(ScramFunctions.serverSignature(MECH, serverKey, authMessage),
            Base64.getDecoder().decode(attribute(serverFinal, "v")));
        assertNull(authenticator.authenticatedUsername());

        AuthenticatorResult third = authenticator.processStep(new byte[0]);
        assertTrue(third.isCompleted());
        assertEquals("bob", authenticator.authenticatedUsername());
    }

    @Test
    void wrongPasswordFails() {
        final String clientFirstBare = "n=bob,r=" + CLIENT_NONCE;
        final String serverFirst = text(authenticator.processStep(bytes(GS2_HEADER + clientFirstBare)));

        AuthenticatorResult result = authenticator.processStep(bytes(clientFinal(clientFirstBare, serverFirst, "wrong")));
        assertTrue(result.isFailure());
        assertNull(authenticator.authenticatedUsername());
    }

    @Test
    void unknownUserGetsAChallengeThenFails() {
        final String clientFirstBare = "n=mallory,r=" + CLIENT_NONCE;
        AuthenticatorResult first = authenticator.processStep(bytes(GS2_HEADER + clientFirstBare));
        assertFalse(first.isFailure(), "must not reveal that the user doesn't exist");

        AuthenticatorResult second = authenticator.processStep(bytes(clientFinal(clientFirstBare, text(first), "secret")));
        assertTrue(second.isFailure());
    }

    @Test
    void channelBindingRequiredIsRejected() {
        assertTrue(authenticator.processStep(bytes("p=tls-server-end-point,,n=bob,r=" + CLIENT_NONCE)).isFailure());
    }

    @Test
    void malformedClientFirstFails() {
        assertTrue(authenticator.processStep(bytes("garbage")).isFailure());
    }

    @Test
    void nonceNotMatchingFails() {
        final String clientFirstBare = "n=bob,r=" + CLIENT_NONCE;
        final String serverFirst = text(authenticator.processStep(bytes(GS2_HEADER + clientFirstBare)));
        final String tampered = serverFirst.replaceFirst("r=" + CLIENT_NONCE, "r=OTHERNONCE");

        assertTrue(authenticator.processStep(bytes(clientFinal(clientFirstBare, tampered, "secret"))).isFailure());
    }

    @Test
    void invalidBase64ProofFails() {
        final String clientFirstBare = "n=bob,r=" + CLIENT_NONCE;
        final String serverFirst = text(authenticator.processStep(bytes(GS2_HEADER + clientFirstBare)));
        final String message = "c=" + Base64.getEncoder().encodeToString(bytes(GS2_HEADER))
            + ",r=" + attribute(serverFirst, "r") + ",p=%%%";

        assertTrue(authenticator.processStep(bytes(message)).isFailure());
    }

    @Test
    void everyStepAfterAFailureFails() {
        assertTrue(authenticator.processStep(bytes("garbage")).isFailure());
        assertTrue(authenticator.processStep(bytes(GS2_HEADER + "n=bob,r=" + CLIENT_NONCE)).isFailure());
        assertTrue(authenticator.processStep(new byte[0]).isFailure());
    }
}
