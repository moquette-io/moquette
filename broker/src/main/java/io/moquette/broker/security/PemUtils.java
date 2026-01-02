/*
 * Copyright (c) 2023 The original author or authors
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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.util.Base64;

public final class PemUtils {
    private static final String certificateBoundaryType = "CERTIFICATE";

    public static String certificatesToPem(Certificate... certificates)
        throws CertificateEncodingException, IOException {
        try (StringWriter str = new StringWriter();
               PemWriter pemWriter = new PemWriter(str)) {
            for (Certificate certificate : certificates) {
                pemWriter.writeObject(certificateBoundaryType, certificate.getEncoded());
            }
            pemWriter.close();
            return str.toString();
        }
    }

    /**
     * Copyright (c) 2000 - 2021 The Legion of the Bouncy Castle Inc. (https://www.bouncycastle.org)
     * SPDX-License-Identifier: MIT
     *
     * <p>A generic PEM writer, based on RFC 1421
     * From: https://javadoc.io/static/org.bouncycastle/bcprov-jdk15on/1.62/org/bouncycastle/util/io/pem/PemWriter.html</p>
     */
    public static class PemWriter extends BufferedWriter {
        private static final int LINE_LENGTH = 64;
        private final char[] buf = new char[LINE_LENGTH];

        /**
         * Base constructor.
         *
         * @param out output stream to use.
         */
        public PemWriter(Writer out) {
            super(out);
        }

        /**
         * Writes a pem encoded string.
         *
         * @param type  key type.
         * @param bytes encoded string
         * @throws IOException IO Exception
         */
        public void writeObject(String type, byte[] bytes) throws IOException {
            writePreEncapsulationBoundary(type);
            writeEncoded(bytes);
            writePostEncapsulationBoundary(type);
        }

        private void writeEncoded(byte[] bytes) throws IOException {
            bytes = Base64.getEncoder().encode(bytes);
            for (int i = 0; i < bytes.length; i += buf.length) {
                int index = 0;
                while (index != buf.length) {
                    if ((i + index) >= bytes.length) {
                        break;
                    }
                    buf[index] = (char) bytes[i + index];
                    index++;
                }
                this.write(buf, 0, index);
                this.newLine();
            }
        }

        private void writePreEncapsulationBoundary(String type) throws IOException {
            this.write("-----BEGIN " + type + "-----");
            this.newLine();
        }

        private void writePostEncapsulationBoundary(String type) throws IOException {
            this.write("-----END " + type + "-----");
            this.newLine();
        }
    }
}
