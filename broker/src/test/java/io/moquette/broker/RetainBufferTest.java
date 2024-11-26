/*
 *
 *  * Copyright (c) 2012-2024 The original author or authors
 *  * ------------------------------------------------------
 *  * All rights reserved. This program and the accompanying materials
 *  * are made available under the terms of the Eclipse Public License v1.0
 *  * and Apache License v2.0 which accompanies this distribution.
 *  *
 *  * The Eclipse Public License is available at
 *  * http://www.eclipse.org/legal/epl-v10.html
 *  *
 *  * The Apache License v2.0 is available at
 *  * http://www.opensource.org/licenses/apache2.0.php
 *  *
 *  * You may elect to redistribute this code under either of these licenses.
 *
 */

package io.moquette.broker;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RetainBufferTest {


    @Test
    public void testRetainedDuplicate() {
        ByteBuf origin = Unpooled.buffer(10);
        assertEquals(1, origin.refCnt());

        ByteBuf retainedDup = origin.retainedDuplicate();
        assertEquals(2, origin.refCnt());
        assertEquals(2, retainedDup.refCnt());
    }

    @Test
    public void testDuplicate() {
        ByteBuf origin = Unpooled.buffer(10);
        assertEquals(1, origin.refCnt());

        ByteBuf duplicate = origin.duplicate();
        assertEquals(1, origin.refCnt());
        assertEquals(1, duplicate.refCnt());
    }
}
