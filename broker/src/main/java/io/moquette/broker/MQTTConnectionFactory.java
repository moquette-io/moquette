package io.moquette.broker;

import io.netty.channel.Channel;

class MQTTConnectionFactory {

    public MQTTConnection create(Channel channel) {
        return new MQTTConnection(channel);
    }
}
