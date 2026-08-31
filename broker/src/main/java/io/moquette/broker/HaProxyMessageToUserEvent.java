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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.haproxy.HAProxyMessage;

/**
 * Converts {@link HAProxyMessage} from channelRead to userEvent.
 *
 * <p>{@code HAProxyMessageDecoder} fires decoded PROXY messages via {@code out.add()},
 * which triggers {@code ctx.fireChannelRead()}. However, downstream handlers
 * (e.g. MQTT decoder, MQTT handler) expect only MQTT messages via channelRead.
 *
 * <p>This handler intercepts {@code HAProxyMessage} in channelRead and re-fires it
 * as a {@code userEvent}, which is the correct event type for protocol handshake
 * messages that should not be processed as application-level data.
 */
@ChannelHandler.Sharable
class HaProxyMessageToUserEvent extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HAProxyMessage) {
            // Re-fire as userEvent so downstream handlers can process it correctly
            ctx.fireUserEventTriggered(msg);
        } else {
            ctx.fireChannelRead(msg);
        }
    }
}
