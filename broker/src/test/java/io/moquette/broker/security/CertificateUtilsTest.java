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

package io.moquette.broker.security;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.security.KeyStore;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CertificateUtilsTest {

    @Test
    public void readsTheSubjectCnOfAnX509Certificate() throws Exception {
        KeyStore ks = KeyStore.getInstance("JKS");
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("signedclientkeystore.jks")) {
            ks.load(in, "passw0rd".toCharArray());
        }
        // generated with -dname cn=client.moquette.io, see ServerIntegrationSSLClientAuthBase
        Certificate cert = ks.getCertificate("signedtestclient");

        assertEquals("client.moquette.io", CertificateUtils.subjectCommonName(cert));
    }

    @Test
    public void refusesACertificateThatIsNotX509() {
        Certificate notX509 = new Certificate("Opaque") {
            @Override
            public byte[] getEncoded() throws CertificateEncodingException {
                return new byte[0];
            }

            @Override
            public void verify(PublicKey key) {
            }

            @Override
            public void verify(PublicKey key, String sigProvider) {
            }

            @Override
            public String toString() {
                return "opaque";
            }

            @Override
            public PublicKey getPublicKey() {
                return null;
            }
        };

        assertThrows(IllegalArgumentException.class, () -> CertificateUtils.subjectCommonName(notX509));
    }
}
