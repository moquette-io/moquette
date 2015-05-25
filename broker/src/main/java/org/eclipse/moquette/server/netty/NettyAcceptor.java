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
package org.eclipse.moquette.server.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;

import java.io.*;
import java.net.URL;
import java.security.KeyStore;

import java.util.List;
import java.util.Properties;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import org.eclipse.moquette.commons.Constants;
import org.eclipse.moquette.spi.IMessaging;
import org.eclipse.moquette.parser.netty.MQTTDecoder;
import org.eclipse.moquette.parser.netty.MQTTEncoder;
import org.eclipse.moquette.server.ServerAcceptor;
import org.eclipse.moquette.server.netty.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author andrea
 */
public class NettyAcceptor implements ServerAcceptor {
    
    static class WebSocketFrameToByteBufDecoder extends MessageToMessageDecoder<BinaryWebSocketFrame> {

        @Override
        protected void decode(ChannelHandlerContext chc, BinaryWebSocketFrame frame, List<Object> out) throws Exception {
            //convert the frame to a ByteBuf
            ByteBuf bb = frame.content();
            //System.out.println("WebSocketFrameToByteBufDecoder decode - " + ByteBufUtil.hexDump(bb));
            bb.retain();
            out.add(bb);
        }
    }
    
    static class ByteBufToWebSocketFrameEncoder extends MessageToMessageEncoder<ByteBuf> {

        @Override
        protected void encode(ChannelHandlerContext chc, ByteBuf bb, List<Object> out) throws Exception {
            //convert the ByteBuf to a WebSocketFrame
            BinaryWebSocketFrame result = new BinaryWebSocketFrame();
            //System.out.println("ByteBufToWebSocketFrameEncoder encode - " + ByteBufUtil.hexDump(bb));
            result.content().writeBytes(bb);
            out.add(result);
        }
    }

    abstract class PipelineInitializer {

        abstract void init(ChannelPipeline pipeline) throws Exception;
    }

    private static final Logger LOG = LoggerFactory.getLogger(NettyAcceptor.class);
    
    EventLoopGroup m_bossGroup;
    EventLoopGroup m_workerGroup;
    //BytesMetricsCollector m_metricsCollector = new BytesMetricsCollector();
    MessageMetricsCollector m_metricsCollector = new MessageMetricsCollector();

    @Override
    public void initialize(IMessaging messaging, Properties props) throws IOException {
        m_bossGroup = new NioEventLoopGroup();
        m_workerGroup = new NioEventLoopGroup();
        
        initializePlainTCPTransport(messaging, props);
        initializeWebSocketTransport(messaging, props);
        initializeSSLTCPTransport(messaging, props);
    }

    private void initFactory(String host, int port, final PipelineInitializer pipeliner) {
        ServerBootstrap b = new ServerBootstrap();
        b.group(m_bossGroup, m_workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        try {
                            pipeliner.init(pipeline);
                        } catch (Throwable th) {
                            LOG.error("Severe error during pipeline creation", th);
                            throw th;
                        }
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true);
        try {
            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(host, port);
            LOG.info("Server binded host: {}, port: {}", host, port);
            f.sync();
        } catch (InterruptedException ex) {
            LOG.error(null, ex);
        }
    }
    
    private void initializePlainTCPTransport(IMessaging messaging, Properties props) throws IOException {
        final NettyMQTTHandler handler = new NettyMQTTHandler();
        handler.setMessaging(messaging);
        String host = props.getProperty(Constants.HOST_PROPERTY_NAME);
        int port = Integer.parseInt(props.getProperty(Constants.PORT_PROPERTY_NAME));
        initFactory(host, port, new PipelineInitializer() {
            @Override
            void init(ChannelPipeline pipeline) {
                pipeline.addFirst("idleStateHandler", new IdleStateHandler(0, 0, Constants.DEFAULT_CONNECT_TIMEOUT));
                pipeline.addAfter("idleStateHandler", "idleEventHandler", new MoquetteIdleTimoutHandler());
                //pipeline.addLast("logger", new LoggingHandler("Netty", LogLevel.ERROR));
                pipeline.addLast("decoder", new MQTTDecoder());
                pipeline.addLast("encoder", new MQTTEncoder());
                pipeline.addLast("metrics", new MessageMetricsHandler(m_metricsCollector));
                pipeline.addLast("handler", handler);
            }
        });
    }
    
    private void initializeWebSocketTransport(IMessaging messaging, Properties props) throws IOException {
        String webSocketPortProp = props.getProperty(Constants.WEB_SOCKET_PORT_PROPERTY_NAME);
        if (webSocketPortProp == null) {
            //Do nothing no WebSocket configured
            LOG.info("WebSocket is disabled");
            return;
        }
        int port = Integer.parseInt(webSocketPortProp);
        
        final NettyMQTTHandler handler = new NettyMQTTHandler();
        handler.setMessaging(messaging);

        String host = props.getProperty(Constants.HOST_PROPERTY_NAME);
        initFactory(host, port, new PipelineInitializer() {
            @Override
            void init(ChannelPipeline pipeline) {
                pipeline.addLast("httpEncoder", new HttpResponseEncoder());
                pipeline.addLast("httpDecoder", new HttpRequestDecoder());
                pipeline.addLast("aggregator", new HttpObjectAggregator(65536));
                pipeline.addLast("webSocketHandler", new WebSocketServerProtocolHandler("/mqtt"/*"/mqtt"*/, "mqttv3.1, mqttv3.1.1"));
                //pipeline.addLast("webSocketHandler", new WebSocketServerProtocolHandler(null, "mqtt"));
                pipeline.addLast("ws2bytebufDecoder", new WebSocketFrameToByteBufDecoder());
                pipeline.addLast("bytebuf2wsEncoder", new ByteBufToWebSocketFrameEncoder());
                pipeline.addFirst("idleStateHandler", new IdleStateHandler(0, 0, Constants.DEFAULT_CONNECT_TIMEOUT));
                pipeline.addAfter("idleStateHandler", "idleEventHandler", new MoquetteIdleTimoutHandler());
                pipeline.addLast("decoder", new MQTTDecoder());
                pipeline.addLast("encoder", new MQTTEncoder());
                pipeline.addLast("metrics", new MessageMetricsHandler(m_metricsCollector));
                pipeline.addLast("handler", handler);
            }
        });
    }
    
    
    private void initializeSSLTCPTransport(IMessaging messaging, Properties props) throws IOException {
        String sslPortProp = props.getProperty(Constants.SSL_PORT_PROPERTY_NAME);
        if (sslPortProp == null) {
            //Do nothing no SSL configured
            LOG.info("SSL is disabled");
            return;
        }
        final String jksPath = props.getProperty(Constants.JKS_PATH_PROPERTY_NAME);
        if (jksPath == null || jksPath.isEmpty()) {
            //key_store_password or key_manager_password are empty
            LOG.warn("You have configured the SSL port but not the jks_path, SSL not started");
            return;
        }
        
        //if we have the port also the jks then keyStorePassword and keyManagerPassword 
        //has to be defined
        final String keyStorePassword = props.getProperty(Constants.KEY_STORE_PASSWORD_PROPERTY_NAME);
        final String keyManagerPassword = props.getProperty(Constants.KEY_MANAGER_PASSWORD_PROPERTY_NAME);
        if (keyStorePassword == null || keyStorePassword.isEmpty()) {
            //key_store_password or key_manager_password are empty
            LOG.warn("You have configured the SSL port but not the key_store_password, SSL not started");
            return;
        }
        if (keyManagerPassword == null || keyManagerPassword.isEmpty()) {
            //key_manager_password or key_manager_password are empty
            LOG.warn("You have configured the SSL port but not the key_manager_password, SSL not started");
            return;
        }

        int sslPort = Integer.parseInt(sslPortProp);
        String host = props.getProperty(Constants.HOST_PROPERTY_NAME);
        LOG.info("Starting SSL on port {} using keystore at {}", sslPort, jksPath);

        final NettyMQTTHandler handler = new NettyMQTTHandler();
        handler.setMessaging(messaging);
        initFactory(host, sslPort, new PipelineInitializer() {
            @Override
            void init(ChannelPipeline pipeline) throws Exception {
                InputStream jksInputStream = jksDatastore(jksPath);
                SSLContext serverContext = SSLContext.getInstance("TLS");
                final KeyStore ks = KeyStore.getInstance("JKS");
                ks.load(jksInputStream, keyStorePassword.toCharArray());
                final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(ks, keyManagerPassword.toCharArray());
                serverContext.init(kmf.getKeyManagers(), null, null);

                SSLEngine engine = serverContext.createSSLEngine();
                engine.setUseClientMode(false);
                final SslHandler sslHandler = new SslHandler(engine);

                pipeline.addLast("ssl", sslHandler);
                //pipeline.addFirst("metrics", new BytesMetricsHandler(m_metricsCollector));
                pipeline.addFirst("idleStateHandler", new IdleStateHandler(0, 0, Constants.DEFAULT_CONNECT_TIMEOUT));
                pipeline.addAfter("idleStateHandler", "idleEventHandler", new MoquetteIdleTimoutHandler());
                //pipeline.addLast("logger", new LoggingHandler("Netty", LogLevel.ERROR));
                pipeline.addLast("decoder", new MQTTDecoder());
                pipeline.addLast("encoder", new MQTTEncoder());
                pipeline.addLast("metrics", new MessageMetricsHandler(m_metricsCollector));
                pipeline.addLast("handler", handler);
            }
        });
    }

    public void close() {
        if (m_workerGroup == null) {
            throw new IllegalStateException("Invoked close on an Acceptor that wasn't initialized");
        }
        if (m_bossGroup == null) {
            throw new IllegalStateException("Invoked close on an Acceptor that wasn't initialized");
        }
        m_workerGroup.shutdownGracefully();
        m_bossGroup.shutdownGracefully();

        MessageMetrics metrics = m_metricsCollector.computeMetrics();
        //LOG.info(String.format("Bytes read: %d, bytes wrote: %d", metrics.readBytes(), metrics.wroteBytes()));
        LOG.info("Msg read: {}, msg wrote: {}", metrics.messagesRead(), metrics.messagesWrote());
    }

    private InputStream jksDatastore(String jksPath) throws FileNotFoundException {
        URL jksUrl = getClass().getClassLoader().getResource(jksPath);
        if (jksUrl != null) {
            LOG.info("Starting with jks at {}, jks normal {}", jksUrl.toExternalForm(), jksUrl);
            return getClass().getClassLoader().getResourceAsStream(jksPath);
        }
        LOG.info("jks not found in bundled resources, try on the filesystem");
        File jksFile = new File(jksPath);
        if (jksFile.exists()) {
            LOG.info("Using {} ", jksFile.getAbsolutePath());
            return new FileInputStream(jksFile);
        }
        LOG.warn("File {} doesn't exists", jksFile.getAbsolutePath());
        return null;
    }
    
}
