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

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

public final class CertificateUtils {

    private CertificateUtils() {
    }

    /**
     * The subject Common Name of an X.509 certificate.
     *
     * @throws IllegalArgumentException when the certificate is not X.509 or its subject carries no CN.
     */
    public static String subjectCommonName(Certificate certificate) {
        if (!(certificate instanceof X509Certificate)) {
            throw new IllegalArgumentException("Not an X.509 certificate: " + certificate.getType());
        }
        final String subject = ((X509Certificate) certificate).getSubjectX500Principal().getName();
        try {
            for (Rdn rdn : new LdapName(subject).getRdns()) {
                if ("CN".equalsIgnoreCase(rdn.getType())) {
                    return rdn.getValue().toString();
                }
            }
        } catch (InvalidNameException e) {
            throw new IllegalArgumentException("Unparseable certificate subject: " + subject, e);
        }
        throw new IllegalArgumentException("Certificate subject has no CN: " + subject);
    }
}
