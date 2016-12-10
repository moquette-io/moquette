/*
 * Copyright (c) 2012-2015 The original author or authors
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
package io.moquette.spi.impl;

import io.moquette.BrokerConstants;
import io.moquette.server.Server;
import io.moquette.spi.IMessagesStore;
import io.moquette.interception.InterceptHandler;
import io.moquette.server.config.IConfig;
import io.moquette.spi.ISessionsStore;
import io.moquette.spi.impl.security.*;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.SubscriptionsStore;
import io.moquette.spi.persistence.MapDBPersistentStore;
import io.moquette.spi.security.IAuthenticator;
import io.moquette.spi.security.IAuthorizator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * It's main responsibility is bootstrap the ProtocolProcessor.
 *
 * @author andrea
 */
public class ProtocolProcessorBootstrapper {

    private static final Logger LOG = LoggerFactory.getLogger(ProtocolProcessorBootstrapper.class);

    private SubscriptionsStore subscriptions;

    private MapDBPersistentStore m_mapStorage;
    
    private ISessionsStore m_sessionsStore;

    private BrokerInterceptor m_interceptor;

    private final ProtocolProcessor m_processor = new ProtocolProcessor();

    public ProtocolProcessorBootstrapper() {
    }

    /**
     * Initialize the processing part of the broker.
     * @param props the properties carrier where some props like port end host could be loaded.
     *              For the full list check of configurable properties check moquette.conf file.
     * @param embeddedObservers a list of callbacks to be notified of certain events inside the broker.
     *                          Could be empty list of null.
     * @param authenticator an implementation of the authenticator to be used, if null load that specified in config
     *                      and fallback on the default one (permit all).
     * @param authorizator an implementation of the authorizator to be used, if null load that specified in config
     *                      and fallback on the default one (permit all).
     * */
    public ProtocolProcessor init(IConfig props, List<? extends InterceptHandler> embeddedObservers,
                                  IAuthenticator authenticator, IAuthorizator authorizator, Server server) {
        subscriptions = new SubscriptionsStore();

        m_mapStorage = new MapDBPersistentStore(props);
        m_mapStorage.initStore();
        IMessagesStore messagesStore = m_mapStorage.messagesStore();
        m_sessionsStore = m_mapStorage.sessionsStore();

        List<InterceptHandler> observers = new ArrayList<>(embeddedObservers);
        String interceptorClassName = props.getProperty(BrokerConstants.INTERCEPT_HANDLER_PROPERTY_NAME);
        if (interceptorClassName != null && !interceptorClassName.isEmpty()) {
            try {
                InterceptHandler handler;
                try {
                    final Constructor<? extends InterceptHandler> constructor = Class.forName(interceptorClassName).asSubclass(InterceptHandler.class).getConstructor(Server.class);
                    handler = constructor.newInstance(server);
                } catch (NoSuchMethodException nsme){
                    handler = Class.forName(interceptorClassName).asSubclass(InterceptHandler.class).newInstance();
                }
                observers.add(handler);
            } catch (Throwable ex) {
                LOG.error("Can't load the intercept handler {}", ex);
            }
        }
        m_interceptor = new BrokerInterceptor(observers);

        subscriptions.init(m_sessionsStore);

        String configPath = System.getProperty("moquette.path", null);
        String authenticatorClassName = props.getProperty(BrokerConstants.AUTHENTICATOR_CLASS_NAME, "");

        if (!authenticatorClassName.isEmpty()) {
            authenticator = (IAuthenticator)loadClass(authenticatorClassName, IAuthenticator.class, props);
            LOG.info("Loaded custom authenticator {}", authenticatorClassName);
        }

        if (authenticator == null) {
            String passwdPath = props.getProperty(BrokerConstants.PASSWORD_FILE_PROPERTY_NAME, "");
            if (passwdPath.isEmpty()) {
                authenticator = new AcceptAllAuthenticator();
            } else {
                authenticator = new FileAuthenticator(configPath, passwdPath);
            }
        }

        String authorizatorClassName = props.getProperty(BrokerConstants.AUTHORIZATOR_CLASS_NAME, "");
        if (!authorizatorClassName.isEmpty()) {
            authorizator = (IAuthorizator)loadClass(authorizatorClassName, IAuthorizator.class, props);
            LOG.info("Loaded custom authorizator {}", authorizatorClassName);
        }

        if (authorizator == null) {
            String aclFilePath = props.getProperty(BrokerConstants.ACL_FILE_PROPERTY_NAME, "");
            if (aclFilePath != null && !aclFilePath.isEmpty()) {
                authorizator = new DenyAllAuthorizator();
                File aclFile = new File(configPath, aclFilePath);
                try {
                    authorizator = ACLFileParser.parse(aclFile);
                } catch (ParseException pex) {
                    LOG.error(String.format("Format error in parsing acl file %s", aclFile), pex);
                }
                LOG.info("Using acl file defined at path {}", aclFilePath);
            } else {
                authorizator = new PermitAllAuthorizator();
                LOG.info("Starting without ACL definition");
            }

        }

        boolean allowAnonymous = Boolean.parseBoolean(props.getProperty(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, "true"));
        boolean allowZeroByteClientId = Boolean.parseBoolean(props.getProperty(BrokerConstants.ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME, "false"));
        m_processor.init(subscriptions, messagesStore, m_sessionsStore, authenticator, allowAnonymous, allowZeroByteClientId, authorizator, m_interceptor, props.getProperty(BrokerConstants.PORT_PROPERTY_NAME));
        return m_processor;
    }
    
    private Object loadClass(String className, Class<?> cls, IConfig props) {
        Object instance = null;
        try {
            Class<?> clazz = Class.forName(className);

            // check if method getInstance exists
            Method method = clazz.getMethod("getInstance", new Class[] {});
            try {
                instance = method.invoke(null, new Object[] {});
            } catch (IllegalArgumentException | InvocationTargetException | IllegalAccessException ex) {
                LOG.error(null, ex);
                throw new RuntimeException("Cannot call method "+ className +".getInstance", ex);
            }
        }
        catch (NoSuchMethodException nsmex) {
            try {
                // check if constructor with IConfig parameter exists
                instance = this.getClass().getClassLoader()
                        .loadClass(className)
                        .asSubclass(cls)
                        .getConstructor(IConfig.class).newInstance(props);
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException ex) {
                LOG.error(null, ex);
                throw new RuntimeException("Cannot load custom authenticator class " + className, ex);
            } catch (NoSuchMethodException | InvocationTargetException e) {
                try {
                    // fallback to default constructor
                    instance = this.getClass().getClassLoader()
                            .loadClass(className)
                            .asSubclass(cls)
                            .newInstance();
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException ex) {
                    LOG.error(null, ex);
                    throw new RuntimeException("Cannot load custom authenticator class " + className, ex);
                }
            }
        } catch (ClassNotFoundException ex) {
            LOG.error(null, ex);
            throw new RuntimeException("Class " + className + " not found", ex);
        } catch (SecurityException ex) {
            LOG.error(null, ex);
            throw new RuntimeException("Cannot call method "+ className +".getInstance", ex);
        }

        return instance;
    }

    public List<Subscription> getSubscriptions() {
        return m_sessionsStore.getSubscriptions();
    }

    public void shutdown() {
        this.m_mapStorage.close();
    }
}
