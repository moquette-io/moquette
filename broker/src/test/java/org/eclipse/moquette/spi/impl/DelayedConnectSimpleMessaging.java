package org.eclipse.moquette.spi.impl;

import org.eclipse.moquette.proto.messages.ConnectMessage;
import org.eclipse.moquette.server.ServerChannel;

public class DelayedConnectSimpleMessaging extends SimpleMessaging {
    private final long connectDelayMilliseconds;
    public DelayedConnectSimpleMessaging(long connectDelayMilliseconds) {
        super();
        this.connectDelayMilliseconds = connectDelayMilliseconds;
    }
    @Override
    protected ProtocolProcessor createProtocolProcessor() {
        return new ProtocolProcessor() {
            @Override
            @MQTTMessage(message = ConnectMessage.class)
            void processConnect(ServerChannel session, ConnectMessage msg) {
                try {
                    Thread.sleep(connectDelayMilliseconds);
                } catch (InterruptedException e) {
                }
                super.processConnect(session, msg);
            }
        };
    }
}
