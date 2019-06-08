/*
 *
 * Copyright (c) 2019, 4NG and/or its affiliates. All rights reserved.
 * 4NG PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 */
package io.moquette.interception.messages;

public class InterceptPingRequestMessage {
    private final String clientID;

    public InterceptPingRequestMessage(String clientID) {
        this.clientID = clientID;
    }

    public String getClientID() {
        return clientID;
    }
}
