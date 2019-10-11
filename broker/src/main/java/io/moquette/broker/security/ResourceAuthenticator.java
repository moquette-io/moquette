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

package io.moquette.broker.security;

import io.moquette.BrokerConstants;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.IResourceLoader;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Load user credentials from a text resource. Each line of the file is formatted as
 * "[username]:[sha256(password)]". The username mustn't contains : char.
 *
 * To encode your password from command line on Linux systems, you could use:
 *
 * <pre>
 *     echo -n "yourpassword" | sha256sum
 * </pre>
 *
 * NB -n is important because echo append a newline by default at the of string. -n avoid this
 * behaviour.
 */
public class ResourceAuthenticator implements IAuthenticator {

    protected static final Logger LOG = LoggerFactory.getLogger(ResourceAuthenticator.class);

    private Map<String, String> m_identities = new HashMap<>();

    private final ExecutorService executor;

    public ResourceAuthenticator(IResourceLoader resourceLoader, String resourceName, IConfig conf) {
        try {
            MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException nsaex) {
            LOG.error("Can't find SHA-256 for password encoding", nsaex);
            throw new RuntimeException(nsaex);
        }

        int authExecutorPoolSize = Integer.parseInt(conf.getProperty(BrokerConstants.AUTH_THREAD_POOL_SIZE, "1"));
        this.executor = Executors.newFixedThreadPool(authExecutorPoolSize);

        LOG.info(String.format("Loading password %s %s", resourceLoader.getName(), resourceName));
        Reader reader = null;
        try {
            reader = resourceLoader.loadResource(resourceName);
            if (reader == null) {
                LOG.warn(String.format("Parsing not existing %s %s", resourceLoader.getName(), resourceName));
            } else {
                parse(reader);
            }
        } catch (IResourceLoader.ResourceIsDirectoryException e) {
            LOG.warn(String.format("Trying to parse directory %s", resourceName));
        } catch (ParseException pex) {
            LOG.warn(
                    String.format("Format error in parsing password %s %s", resourceLoader.getName(), resourceName),
                    pex);
        }
    }

    private void parse(Reader reader) throws ParseException {
        if (reader == null) {
            return;
        }

        BufferedReader br = new BufferedReader(reader);
        String line;
        try {
            while ((line = br.readLine()) != null) {
                int commentMarker = line.indexOf('#');
                if (commentMarker != -1) {
                    if (commentMarker == 0) {
                        // skip its a comment
                        continue;
                    } else {
                        // it's a malformed comment
                        throw new ParseException(line, commentMarker);
                    }
                } else {
                    if (line.isEmpty() || line.matches("^\\s*$")) {
                        // skip it's a black line
                        continue;
                    }

                    // split till the first space
                    int delimiterIdx = line.indexOf(':');
                    String username = line.substring(0, delimiterIdx).trim();
                    String password = line.substring(delimiterIdx + 1).trim();

                    m_identities.put(username, password);
                }
            }
        } catch (IOException ex) {
            throw new ParseException("Failed to read", 1);
        }
    }

    @Override
    public CompletableFuture<Boolean> checkValid(String clientId, String username, byte[] password) {
        return CompletableFuture.supplyAsync(() -> {
            if (username == null || password == null) {
                LOG.info("username or password was null");
                return false;
            }
            String foundPwq = m_identities.get(username);
            if (foundPwq == null) {
                return false;
            }
            String encodedPasswd = DigestUtils.sha256Hex(password);
            return foundPwq.equals(encodedPasswd);
        }, executor);
    }
}
