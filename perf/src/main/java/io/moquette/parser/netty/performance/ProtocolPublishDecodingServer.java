package io.moquette.parser.netty.performance;

import io.moquette.parser.netty.MQTTDecoder;
import io.moquette.parser.netty.MQTTEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emulates a broker, but doesn't apply any protocol logic, just forward the qos0 publishes
 * to the lone subscriber, it's used just to measure the protocol decoding/encoding overhead.
 *
 */
public class ProtocolPublishDecodingServer {

    private static final Logger LOG = LoggerFactory.getLogger(ProtocolPublishDecodingServer.class);

    EventLoopGroup m_bossGroup;
    EventLoopGroup m_workerGroup;

    void init() {
        String host = "0.0.0.0";
        int port = 1883;
        m_bossGroup = new NioEventLoopGroup();
        m_workerGroup = new NioEventLoopGroup();

        ServerBootstrap b = new ServerBootstrap();
        b.group(m_bossGroup, m_workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        try {
                            pipeline.addLast("decoder", new MQTTDecoder());
                            pipeline.addLast("encoder", new MQTTEncoder());
                            pipeline.addLast("handler", new PublishReceiverHandler());
//                            pipeline.addLast("decoder", new MqttDecoder());
//                            pipeline.addLast("encoder", MqttEncoder.INSTANCE);
//                            pipeline.addLast("handler", new NettyPublishReceiverHandler());
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


    public void stop() {
        if (m_workerGroup == null) {
            throw new IllegalStateException("Invoked close on an Acceptor that wasn't initialized");
        }
        if (m_bossGroup == null) {
            throw new IllegalStateException("Invoked close on an Acceptor that wasn't initialized");
        }
        m_workerGroup.shutdownGracefully();
        m_bossGroup.shutdownGracefully();

        LOG.info("Closed boss and worker Event loops");
        System.out.println("Server stopped");
    }

    public static void main(String[] args) throws MqttException, InterruptedException {
        final ProtocolPublishDecodingServer server = new ProtocolPublishDecodingServer();
        server.init();

        System.out.println("Loop server started");
        publishBombing();
        publishBombing();
        publishBombing();
        publishBombing();
        publishBombing();
        //Bind  a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> server.stop()));
    }

    private static void publishBombing() {
        LOG.info("Started publish loop");
        PublishBomber heavyPublisher = new PublishBomber("localhost", 1883);
        heavyPublisher.publishLoop(25000, 100000);
        heavyPublisher.disconnect();
        LOG.info("Finished publish loop");
    }

}
