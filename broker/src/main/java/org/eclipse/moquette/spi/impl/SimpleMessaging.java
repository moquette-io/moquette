/*
 * Copyright (c) 2012-2014 The original author or authors
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
package org.eclipse.moquette.spi.impl;

import static org.eclipse.moquette.commons.Constants.PASSWORD_FILE_PROPERTY_NAME;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.eclipse.moquette.proto.messages.AbstractMessage;
import org.eclipse.moquette.server.IAuthenticator;
import org.eclipse.moquette.server.ServerChannel;
import org.eclipse.moquette.spi.IMessagesStore;
import org.eclipse.moquette.spi.IMessaging;
import org.eclipse.moquette.spi.ISessionsStore;
import org.eclipse.moquette.spi.impl.events.LostConnectionEvent;
import org.eclipse.moquette.spi.impl.events.MessagingEvent;
import org.eclipse.moquette.spi.impl.events.ProtocolEvent;
import org.eclipse.moquette.spi.impl.events.StopEvent;
import org.eclipse.moquette.spi.impl.subscriptions.SubscriptionsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 *
 * Singleton class that orchestrate the execution of the protocol.
 *
 * Uses the LMAX Disruptor to serialize the incoming, requests, because it work in a evented fashion;
 * the requests income from front Netty connectors and are dispatched to the 
 * ProtocolProcessor.
 *
 * @author andrea
 */
public class SimpleMessaging implements IMessaging, EventHandler<ValueEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleMessaging.class);
    
    private SubscriptionsStore m_subscriptions;
    
    private RingBuffer<ValueEvent> m_ringBuffer;

    private IMessagesStore m_storageService;
    private ISessionsStore m_sessionsStore;

    private ExecutorService m_executor;
    private Disruptor<ValueEvent> m_disruptor;

    private final ProtocolProcessor m_processor = new ProtocolProcessor();
    private final AnnotationSupport m_annotationSupport = new AnnotationSupport();
    private boolean m_benchmarkEnabled = false;
    
    private CountDownLatch m_stopLatch;

    private Histogram m_histogram = new Histogram(5);
    
    public SimpleMessaging(int ringBufferSize, boolean benchmarkEnabled, IMessagesStore storageService, ISessionsStore sessionsStore, IAuthenticator authenticator) {
        m_executor = Executors.newFixedThreadPool(1);
        m_disruptor = new Disruptor<>(ValueEvent.EVENT_FACTORY, ringBufferSize, m_executor);
        m_disruptor.handleEventsWith(this);
        m_disruptor.start();

        m_ringBuffer = m_disruptor.getRingBuffer();

        m_annotationSupport.processAnnotations(m_processor);
        m_benchmarkEnabled = benchmarkEnabled;
		
		m_storageService = storageService;
		m_sessionsStore = sessionsStore;
		
		m_subscriptions = new SubscriptionsStore();
		m_subscriptions.init(m_sessionsStore);
		
		m_processor.init(m_subscriptions, m_storageService, m_sessionsStore, authenticator);
    }

    
    private void disruptorPublish(MessagingEvent msgEvent) {
        LOG.debug("disruptorPublish publishing event {}", msgEvent);
        long sequence = m_ringBuffer.next();
        ValueEvent event = m_ringBuffer.get(sequence);

        event.setEvent(msgEvent);
        
        m_ringBuffer.publish(sequence); 
    }
    
    @Override
    public void lostConnection(ServerChannel session, String clientID) {
        disruptorPublish(new LostConnectionEvent(session, clientID));
    }

    @Override
    public void handleProtocolMessage(ServerChannel session, AbstractMessage msg) {
        disruptorPublish(new ProtocolEvent(session, msg));
    }

    @Override
    public void stop() {
        m_stopLatch = new CountDownLatch(1);
        disruptorPublish(new StopEvent());
        try {
            //wait the callback notification from the protocol processor thread
            LOG.debug("waiting 10 sec to m_stopLatch");
            boolean elapsed = !m_stopLatch.await(10, TimeUnit.SECONDS);
            LOG.debug("after m_stopLatch");
            m_executor.shutdown();
            m_disruptor.shutdown();
            if (elapsed) {
                LOG.error("Can't stop the server in 10 seconds");
            }
        } catch (InterruptedException ex) {
            LOG.error(null, ex);
        }
    }
    
    @Override
    public void onEvent(ValueEvent t, long l, boolean bln) throws Exception {
        MessagingEvent evt = t.getEvent();
        LOG.info("onEvent processing messaging event from input ringbuffer {}", evt);
        if (evt instanceof StopEvent) {
            processStop();
            return;
        } 
        if (evt instanceof LostConnectionEvent) {
            LostConnectionEvent lostEvt = (LostConnectionEvent) evt;
            m_processor.processConnectionLost(lostEvt);
            return;
        }
        
        if (evt instanceof ProtocolEvent) {
            ServerChannel session = ((ProtocolEvent) evt).getSession();
            AbstractMessage message = ((ProtocolEvent) evt).getMessage();
            try {
                long startTime = System.nanoTime();
                m_annotationSupport.dispatch(session, message);
                if (m_benchmarkEnabled) {
                    long delay = System.nanoTime() - startTime;
                    m_histogram.recordValue(delay);
                }
            } catch (Throwable th) {
                LOG.error("Grave error processing the message {} for {}", message, session, th);
            }
        }
    }

    private void processStop() {
        LOG.debug("processStop invoked");
        m_storageService.close();
        LOG.debug("subscription tree {}", m_subscriptions.dumpTree());
//        m_eventProcessor.halt();
//        m_executor.shutdown();
        
        m_subscriptions = null;
        m_stopLatch.countDown();

        if (m_benchmarkEnabled) {
            //log metrics
            m_histogram.outputPercentileDistribution(System.out, 1000.0);
        }
    }
}
