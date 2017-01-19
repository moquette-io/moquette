/*
 * Copyright (c) 2012-2017 The original author or authorsgetRockQuestions()
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
package io.moquette.parser.netty;

import io.moquette.parser.proto.messages.PubAckMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import io.moquette.parser.proto.messages.AbstractMessage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author andrea
 */
public class PubAckDecoderTest {
    ByteBuf m_buff;
    PubAckDecoder m_msgdec;
    
    @Before
    public void setUp() {
        m_msgdec = new PubAckDecoder();
    }
    
    @Test
    public void testHeader() throws Exception {
        m_buff = Unpooled.buffer(14);
        int messageId = 0xAABB;
        initHeader(m_buff, messageId);
        List<Object> results = new ArrayList<>();
        
        //Excercise
        m_msgdec.decode(null, m_buff, results);
        
        assertFalse(results.isEmpty());
        PubAckMessage message = (PubAckMessage)results.get(0);
        assertNotNull(message);
        assertEquals(messageId, message.getMessageID().intValue());
        assertEquals(AbstractMessage.PUBACK, message.getMessageType());
    }
    
    private void initHeader(ByteBuf buff, int messageID) {
        buff.clear().writeByte(AbstractMessage.PUBACK << 4).writeByte(2);
        
        //return code
        buff.writeShort(messageID);
    }
}
