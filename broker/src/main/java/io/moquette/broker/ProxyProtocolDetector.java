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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Detects whether incoming data is PROXY protocol or application data (e.g. MQTT).
 *
 * <p>Peeks at the first bytes to determine the protocol type without consuming bytes:
 * <ul>
 *   <li>PROXY v2 binary: first byte is 0x0D ({@code \r}), requires 13 bytes for detection</li>
 *   <li>PROXY v1 text: first 6 bytes match "PROXY " ({@code P R O X Y space})</li>
 *   <li>MQTT data: first byte is 0x10-0xF0 (not matching PROXY signatures)</li>
 * </ul>
 *
 * <p>When PROXY protocol is detected, dynamically adds {@link HAProxyMessageDecoder}
 * and {@link HaProxyMessageToUserEvent} after itself and removes itself from the pipeline.
 * The decoder then processes the PROXY header and fires the {@code HAProxyMessage}
 * as a userEvent downstream (via the converter handler).
 *
 * <p>When non-PROXY data is detected:
 * <ul>
 *   <li>If {@code required = false} (auto-detect mode): removes itself, data passes through unchanged</li>
 *   <li>If {@code required = true} (strict mode): closes the connection (PROXY header required)</li>
 * </ul>
 */
public class ProxyProtocolDetector extends ByteToMessageDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(ProxyProtocolDetector.class);

    // "PROXY " prefix for PROXY v1 text protocol
    private static final byte[] PROXY_V1_PREFIX = {0x50, 0x52, 0x4F, 0x58, 0x59, 0x20}; // "PROXY "

    private static final HaProxyMessageToUserEvent HA_PROXY_CONVERTER = new HaProxyMessageToUserEvent();

    private final boolean required;

    /**
     * @param required if true, PROXY protocol header is required; connections without it are rejected.
     *                 if false, auto-detect mode: PROXY protocol if present, otherwise pass through.
     */
    public ProxyProtocolDetector(boolean required) {
        this.required = required;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 6) {
            return; // Not enough data to determine protocol, wait for more
        }

        int firstByte = in.getByte(in.readerIndex());
        if (firstByte == 0x0D && in.readableBytes() >= 13) {
            // PROXY v2 binary protocol prefix starts with \r\n\0\r\nQUIT\n
            addProxyProtocolHandlers(ctx);
            ctx.pipeline().remove(this);
        } else if (firstByte == 0x50 && matchesProxyV1Prefix(in)) {
            // PROXY v1 text protocol starts with "PROXY "
            addProxyProtocolHandlers(ctx);
            ctx.pipeline().remove(this);
        } else {
            // Not PROXY protocol
            if (required) {
                LOG.warn("Rejecting connection: PROXY protocol header required but not received");
                ctx.close();
            } else {
                // Auto-detect mode: pass through unchanged
                ctx.pipeline().remove(this);
            }
        }
    }

    private void addProxyProtocolHandlers(ChannelHandlerContext ctx) {
        ctx.pipeline().addAfter(ctx.name(), "proxyProtocolDecoder",
            new HAProxyMessageDecoder(false));
        ctx.pipeline().addAfter("proxyProtocolDecoder", "haProxyConverter",
            HA_PROXY_CONVERTER);
    }

    private static boolean matchesProxyV1Prefix(ByteBuf in) {
        if (in.readableBytes() < 6) {
            return false;
        }
        int readerIndex = in.readerIndex();
        for (int i = 0; i < PROXY_V1_PREFIX.length; i++) {
            if (in.getByte(readerIndex + i) != PROXY_V1_PREFIX[i]) {
                return false;
            }
        }
        return true;
    }
}
