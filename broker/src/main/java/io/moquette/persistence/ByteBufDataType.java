/*
 * Copyright (c) 2012-2021 The original author or authors
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
package io.moquette.persistence;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;

import java.nio.ByteBuffer;

public final class ByteBufDataType implements org.h2.mvstore.type.DataType {

    @Override
    public int compare(Object a, Object b) {
        return 0;
    }

    @Override
    public int getMemory(Object obj) {
        if (!(obj instanceof ByteBuf)) {
            throw new IllegalArgumentException("Expected instance of ByteBuf but found " + obj.getClass());
        }
        final int payloadSize = ((ByteBuf) obj).readableBytes();
        return 4 + payloadSize;
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    @Override
    public ByteBuf read(ByteBuffer buff) {
        final int payloadSize = buff.getInt();
        byte[] payload = new byte[payloadSize];
        buff.get(payload);
        return Unpooled.wrappedBuffer(payload);
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        final ByteBuf casted = (ByteBuf) obj;
        final int payloadSize = casted.readableBytes();
        byte[] rawBytes = new byte[payloadSize];
        casted.copy().readBytes(rawBytes).release();
        buff.putInt(payloadSize);
        buff.put(rawBytes);
    }
}
