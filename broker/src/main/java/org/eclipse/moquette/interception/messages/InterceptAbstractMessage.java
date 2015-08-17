package org.eclipse.moquette.interception.messages;

import org.eclipse.moquette.proto.messages.AbstractMessage;

/**
 * @author Wagner Macedo
 */
public abstract class InterceptAbstractMessage {
    private final AbstractMessage msg;

    InterceptAbstractMessage(AbstractMessage msg) {
        this.msg = msg;
    }

    public boolean isRetainFlag() {
        return msg.isRetainFlag();
    }

    public boolean isDupFlag() {
        return msg.isDupFlag();
    }

    public AbstractMessage.QOSType getQos() {
        return msg.getQos();
    }
}
