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

package io.moquette;

import io.moquette.broker.config.IConfig;

import java.io.File;

public final class BrokerConstants {

    public static final String INTERCEPT_HANDLER_PROPERTY_NAME = "intercept.handler";
    public static final String BROKER_INTERCEPTOR_THREAD_POOL_SIZE = "intercept.thread_pool.size";
    /**
     * @deprecated use the DATA_PATH_PROPERTY_NAME to define the path where to store
     * the broker files (es queues and subscriptions).
     * Enable persistence with PERSISTENCE_ENABLED_PROPERTY_NAME
     * */
    @Deprecated
    public static final String PERSISTENT_STORE_PROPERTY_NAME = "persistent_store";
    @Deprecated
    public static final String DATA_PATH_PROPERTY_NAME = IConfig.DATA_PATH_PROPERTY_NAME;
    @Deprecated
    public static final String PERSISTENT_QUEUE_TYPE_PROPERTY_NAME = IConfig.PERSISTENT_QUEUE_TYPE_PROPERTY_NAME;
    @Deprecated
    public static final String PERSISTENCE_ENABLED_PROPERTY_NAME = IConfig.PERSISTENCE_ENABLED_PROPERTY_NAME;
    public static final String SEGMENTED_QUEUE_PAGE_SIZE = "queue_page_size";
    public static final int MB = 1024 * 1024;
    public static final int DEFAULT_SEGMENTED_QUEUE_PAGE_SIZE = 64 * MB;
    public static final String SEGMENTED_QUEUE_SEGMENT_SIZE = "queue_segment_size";
    public static final int DEFAULT_SEGMENTED_QUEUE_SEGMENT_SIZE = 4 * MB;
    public static final String AUTOSAVE_INTERVAL_PROPERTY_NAME = "autosave_interval";
    @Deprecated
    public static final String PASSWORD_FILE_PROPERTY_NAME = IConfig.PASSWORD_FILE_PROPERTY_NAME;
    @Deprecated // use IConfig.PORT_PROPERTY_NAME
    public static final String PORT_PROPERTY_NAME = IConfig.PORT_PROPERTY_NAME;
    @Deprecated
    public static final String HOST_PROPERTY_NAME = IConfig.HOST_PROPERTY_NAME;
    public static final String DEFAULT_MOQUETTE_STORE_H2_DB_FILENAME = "moquette_store.h2";
    public static final String DEFAULT_PERSISTENT_PATH = System.getProperty("user.dir") + File.separator
            + DEFAULT_MOQUETTE_STORE_H2_DB_FILENAME;
    @Deprecated
    public static final String WEB_SOCKET_PORT_PROPERTY_NAME = IConfig.WEB_SOCKET_PORT_PROPERTY_NAME;
    @Deprecated
    public static final String WSS_PORT_PROPERTY_NAME = IConfig.WSS_PORT_PROPERTY_NAME;
    @Deprecated
    public static final String WEB_SOCKET_PATH_PROPERTY_NAME = IConfig.WEB_SOCKET_PATH_PROPERTY_NAME;
    public static final String WEB_SOCKET_MAX_FRAME_SIZE_PROPERTY_NAME = "websocket_max_frame_size";
    @Deprecated
    public static final String SESSION_QUEUE_SIZE = IConfig.SESSION_QUEUE_SIZE;
    @Deprecated
    public static final String SSL_PROVIDER = IConfig.SSL_PROVIDER;
    @Deprecated
    public static final String SSL_PORT_PROPERTY_NAME = IConfig.SSL_PORT_PROPERTY_NAME;
    @Deprecated
    public static final String JKS_PATH_PROPERTY_NAME = IConfig.JKS_PATH_PROPERTY_NAME;
    @Deprecated
    public static final String KEY_STORE_TYPE = IConfig.KEY_STORE_TYPE;
    @Deprecated
    public static final String KEY_STORE_PASSWORD_PROPERTY_NAME = IConfig.KEY_STORE_PASSWORD_PROPERTY_NAME;
    @Deprecated
    public static final String KEY_MANAGER_PASSWORD_PROPERTY_NAME = IConfig.KEY_MANAGER_PASSWORD_PROPERTY_NAME;
    @Deprecated
    public static final String ALLOW_ANONYMOUS_PROPERTY_NAME = IConfig.ALLOW_ANONYMOUS_PROPERTY_NAME;
    public static final String REAUTHORIZE_SUBSCRIPTIONS_ON_CONNECT = "reauthorize_subscriptions_on_connect";
    public static final String ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME = "allow_zero_byte_client_id";
    @Deprecated
    public static final String ACL_FILE_PROPERTY_NAME = IConfig.ACL_FILE_PROPERTY_NAME;
    @Deprecated
    public static final String AUTHORIZATOR_CLASS_NAME = IConfig.AUTHORIZATOR_CLASS_NAME;
    @Deprecated
    public static final String AUTHENTICATOR_CLASS_NAME = IConfig.AUTHENTICATOR_CLASS_NAME;
    public static final String DB_AUTHENTICATOR_DRIVER = "authenticator.db.driver";
    public static final String DB_AUTHENTICATOR_URL = "authenticator.db.url";
    public static final String DB_AUTHENTICATOR_QUERY = "authenticator.db.query";
    public static final String DB_AUTHENTICATOR_DIGEST = "authenticator.db.digest";
    public static final int PORT = 1883;
    public static final int WEBSOCKET_PORT = 8080;
    public static final String WEBSOCKET_PATH = "/mqtt";
    public static final String DISABLED_PORT_BIND = "disabled";
    public static final String HOST = "0.0.0.0";
    public static final String NEED_CLIENT_AUTH = "need_client_auth";
    public static final String NETTY_SO_BACKLOG_PROPERTY_NAME = "netty.so_backlog";
    public static final String NETTY_SO_REUSEADDR_PROPERTY_NAME = "netty.so_reuseaddr";
    public static final String NETTY_TCP_NODELAY_PROPERTY_NAME = "netty.tcp_nodelay";
    public static final String NETTY_SO_KEEPALIVE_PROPERTY_NAME = "netty.so_keepalive";
    public static final String NETTY_CHANNEL_TIMEOUT_SECONDS_PROPERTY_NAME = "netty.channel_timeout.seconds";
    public static final String NETTY_NATIVE_PROPERTY_NAME = "netty.native";
    @Deprecated
    public static final String NETTY_MAX_BYTES_PROPERTY_NAME = IConfig.NETTY_MAX_BYTES_PROPERTY_NAME;
    @Deprecated
    public static final int DEFAULT_NETTY_MAX_BYTES_IN_MESSAGE = IConfig.DEFAULT_NETTY_MAX_BYTES_IN_MESSAGE;
    /**
     * @deprecated use the BUFFER_FLUSH_MS_PROPERTY_NAME
     * */
    @Deprecated
    public static final String IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME = "immediate_buffer_flush";
    @Deprecated
    public static final String BUFFER_FLUSH_MS_PROPERTY_NAME = IConfig.BUFFER_FLUSH_MS_PROPERTY_NAME;
    public static final int NO_BUFFER_FLUSH = -1;
    public static final int IMMEDIATE_BUFFER_FLUSH = 0;

    public static final String METRICS_ENABLE_PROPERTY_NAME = "use_metrics";
    public static final String METRICS_LIBRATO_EMAIL_PROPERTY_NAME = "metrics.librato.email";
    public static final String METRICS_LIBRATO_TOKEN_PROPERTY_NAME = "metrics.librato.token";
    public static final String METRICS_LIBRATO_SOURCE_PROPERTY_NAME = "metrics.librato.source";

    @Deprecated
    public static final String ENABLE_TELEMETRY_NAME = IConfig.ENABLE_TELEMETRY_NAME;

    public static final String BUGSNAG_ENABLE_PROPERTY_NAME = "use_bugsnag";
    public static final String BUGSNAG_TOKEN_PROPERTY_NAME = "bugsnag.token";

    public static final String STORAGE_CLASS_NAME = "storage_class";

    @Deprecated
    public static final String PERSISTENT_CLIENT_EXPIRATION_PROPERTY_NAME = IConfig.PERSISTENT_CLIENT_EXPIRATION_PROPERTY_NAME;

    public static final int FLIGHT_BEFORE_RESEND_MS = 5_000;
    public static final int INFLIGHT_WINDOW_SIZE = 10;
    public static final int INFINITE_SESSION_EXPIRY = 0xFFFFFFFF;

    private BrokerConstants() {
    }
}
