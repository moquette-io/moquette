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
    public static final String DATA_PATH_PROPERTY_NAME = "data_path";
    public static final String PERSISTENT_QUEUE_TYPE_PROPERTY_NAME = "persistent_queue_type"; // h2 or segmented, default h2
    public static final String PERSISTENCE_ENABLED_PROPERTY_NAME = "persistence_enabled"; // true or false, default true
    public static final String SEGMENTED_QUEUE_PAGE_SIZE = "queue_page_size";
    public static final int MB = 1024 * 1024;
    public static final int DEFAULT_SEGMENTED_QUEUE_PAGE_SIZE = 64 * MB;
    public static final String SEGMENTED_QUEUE_SEGMENT_SIZE = "queue_segment_size";
    public static final int DEFAULT_SEGMENTED_QUEUE_SEGMENT_SIZE = 4 * MB;
    public static final String AUTOSAVE_INTERVAL_PROPERTY_NAME = "autosave_interval";
    public static final String PASSWORD_FILE_PROPERTY_NAME = "password_file";
    public static final String PORT_PROPERTY_NAME = "port";
    public static final String HOST_PROPERTY_NAME = "host";
    public static final String DEFAULT_MOQUETTE_STORE_H2_DB_FILENAME = "moquette_store.h2";
    public static final String DEFAULT_PERSISTENT_PATH = System.getProperty("user.dir") + File.separator
            + DEFAULT_MOQUETTE_STORE_H2_DB_FILENAME;
    public static final String WEB_SOCKET_PORT_PROPERTY_NAME = "websocket_port";
    public static final String WSS_PORT_PROPERTY_NAME = "secure_websocket_port";
    public static final String WEB_SOCKET_PATH_PROPERTY_NAME = "websocket_path";
    public static final String WEB_SOCKET_MAX_FRAME_SIZE_PROPERTY_NAME = "websocket_max_frame_size";
    public static final String SESSION_QUEUE_SIZE = "session_queue_size";

    /**
     * Defines the SSL implementation to use, default to "JDK".
     * @see io.netty.handler.ssl.SslProvider#name()
     */
    public static final String SSL_PROVIDER = "ssl_provider";
    public static final String SSL_PORT_PROPERTY_NAME = "ssl_port";
    public static final String JKS_PATH_PROPERTY_NAME = "jks_path";

    /** @see java.security.KeyStore#getInstance(String) for allowed types, default to "jks" */
    public static final String KEY_STORE_TYPE = "key_store_type";
    public static final String KEY_STORE_PASSWORD_PROPERTY_NAME = "key_store_password";
    public static final String KEY_MANAGER_PASSWORD_PROPERTY_NAME = "key_manager_password";
    public static final String ALLOW_ANONYMOUS_PROPERTY_NAME = "allow_anonymous";
    public static final String REAUTHORIZE_SUBSCRIPTIONS_ON_CONNECT = "reauthorize_subscriptions_on_connect";
    public static final String ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME = "allow_zero_byte_client_id";
    public static final String ACL_FILE_PROPERTY_NAME = "acl_file";
    public static final String AUTHORIZATOR_CLASS_NAME = "authorizator_class";
    public static final String AUTHENTICATOR_CLASS_NAME = "authenticator_class";
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
    public static final String NETTY_EPOLL_PROPERTY_NAME = "netty.epoll";
    public static final String NETTY_MAX_BYTES_PROPERTY_NAME = "netty.mqtt.message_size";
    public static final int DEFAULT_NETTY_MAX_BYTES_IN_MESSAGE = 8092;
    /**
     * @deprecated use the BUFFER_FLUSH_MS_PROPERTY_NAME
     * */
    @Deprecated
    public static final String IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME = "immediate_buffer_flush";
    /**
     * 0/immediate means immediate flush, like immediate_buffer_flush = true
     * -1/full means no explicit flush, let Netty flush when write buffers are full, like immediate_buffer_flush = false
     * a number of milliseconds to between flushes
     * */
    public static final String BUFFER_FLUSH_MS_PROPERTY_NAME = "buffer_flush_millis";
    public static final int NO_BUFFER_FLUSH = -1;
    public static final int IMMEDIATE_BUFFER_FLUSH = 0;

    public static final String METRICS_ENABLE_PROPERTY_NAME = "use_metrics";
    public static final String METRICS_LIBRATO_EMAIL_PROPERTY_NAME = "metrics.librato.email";
    public static final String METRICS_LIBRATO_TOKEN_PROPERTY_NAME = "metrics.librato.token";
    public static final String METRICS_LIBRATO_SOURCE_PROPERTY_NAME = "metrics.librato.source";

    public static final String ENABLE_TELEMETRY_NAME = "telemetry_enabled";

    public static final String BUGSNAG_ENABLE_PROPERTY_NAME = "use_bugsnag";
    public static final String BUGSNAG_TOKEN_PROPERTY_NAME = "bugsnag.token";

    public static final String STORAGE_CLASS_NAME = "storage_class";

    public static final String PERSISTENT_CLEAN_EXPIRATION_PROPERTY_NAME = "persistent_client_expiration";

    public static final int FLIGHT_BEFORE_RESEND_MS = 5_000;
    public static final int INFLIGHT_WINDOW_SIZE = 10;

    private BrokerConstants() {
    }
}
