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
package io.moquette.broker;

import io.moquette.BrokerConstants;
import io.moquette.broker.config.IConfig;

import java.util.Locale;

class BrokerConfiguration {

    private final boolean allowAnonymous;
    private final boolean peerCertificateAsUsername;
    private final boolean allowZeroByteClientId;
    private final boolean reauthorizeSubscriptionsOnConnect;
    private final int bufferFlushMillis;

    BrokerConfiguration(IConfig props) {
        allowAnonymous = props.boolProp(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, true);
        peerCertificateAsUsername = props.boolProp(IConfig.PEER_CERTIFICATE_AS_USERNAME, false);
        allowZeroByteClientId = props.boolProp(BrokerConstants.ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME, false);
        reauthorizeSubscriptionsOnConnect = props.boolProp(BrokerConstants.REAUTHORIZE_SUBSCRIPTIONS_ON_CONNECT, false);

        // BUFFER_FLUSH_MS_PROPERTY_NAME has precedence over the deprecated IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME
        final String bufferFlushMillisProp = props.getProperty(BrokerConstants.BUFFER_FLUSH_MS_PROPERTY_NAME);
        if (bufferFlushMillisProp != null && !bufferFlushMillisProp.isEmpty()) {
            switch (bufferFlushMillisProp.toLowerCase(Locale.ROOT)) {
                case "immediate":
                    bufferFlushMillis = BrokerConstants. IMMEDIATE_BUFFER_FLUSH;
                    break;
                case "full":
                    bufferFlushMillis = BrokerConstants.NO_BUFFER_FLUSH;
                    break;
                default:
                    final String errorMsg = String.format("Can't state value of %s property. Has to be 'immediate', " +
                        "'full' or a number >= -1, found %s", BrokerConstants.BUFFER_FLUSH_MS_PROPERTY_NAME, bufferFlushMillisProp);
                    try {
                        bufferFlushMillis = Integer.parseInt(bufferFlushMillisProp);
                        if (bufferFlushMillis < -1) {
                            throw new IllegalArgumentException(errorMsg);
                        }
                    } catch (NumberFormatException ex) {
                        throw new IllegalArgumentException(errorMsg);
                    }
            }
        } else {
            if (props.boolProp(BrokerConstants.IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME, true)) {
                bufferFlushMillis = BrokerConstants.IMMEDIATE_BUFFER_FLUSH;
            } else {
                bufferFlushMillis = BrokerConstants.NO_BUFFER_FLUSH;
            }
        }
    }

    public BrokerConfiguration(boolean allowAnonymous, boolean allowZeroByteClientId,
                               boolean reauthorizeSubscriptionsOnConnect, int bufferFlushMillis) {
        this(allowAnonymous, false, allowZeroByteClientId,
            reauthorizeSubscriptionsOnConnect, bufferFlushMillis);
    }

    public BrokerConfiguration(boolean allowAnonymous, boolean peerCertificateAsUsername, boolean allowZeroByteClientId,
                               boolean reauthorizeSubscriptionsOnConnect, int bufferFlushMillis) {
        this.allowAnonymous = allowAnonymous;
        this.peerCertificateAsUsername = peerCertificateAsUsername;
        this.allowZeroByteClientId = allowZeroByteClientId;
        this.reauthorizeSubscriptionsOnConnect = reauthorizeSubscriptionsOnConnect;
        this.bufferFlushMillis = bufferFlushMillis;
    }

    public boolean isAllowAnonymous() {
        return allowAnonymous;
    }

    public boolean isPeerCertificateAsUsername() {
        return peerCertificateAsUsername;
    }

    public boolean isAllowZeroByteClientId() {
        return allowZeroByteClientId;
    }

    public boolean isReauthorizeSubscriptionsOnConnect() {
        return reauthorizeSubscriptionsOnConnect;
    }

    public int getBufferFlushMillis() {
        return bufferFlushMillis;
    }
}
