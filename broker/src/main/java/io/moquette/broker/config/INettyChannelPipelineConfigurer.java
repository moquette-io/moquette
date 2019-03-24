package io.moquette.broker.config;

import io.netty.channel.ChannelPipeline;

public interface INettyChannelPipelineConfigurer {

    default void configure(ChannelPipeline pipeline) {
    }
}
