/*
 * Copyright (c) 2012-2025 The original author or authors
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
package io.moquette.metrics.prometheus;

import java.io.IOException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for fetching data.
 */
public class HttpHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpHelper.class.getName());

    /**
     * Send HTTP GET request to the urlString and return response code and
     * response body
     *
     * @param urlString The URL that the GET request should be sent to
     * @return response-code and response(response body) of the HTTP GET in the
     * MAP format. If the response is not 200, the response(response body) will
     * be empty.
     */
    public static HttpResponse doGet(String urlString) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            return doGet(httpClient, urlString);
        } catch (IOException e) {
            LOGGER.error("Exception: ", e);
        }
        return null;
    }

    /**
     * Send HTTP GET request to the urlString, using the given HttpClient and
     * return response code and response body
     *
     * @param httpClient the HttpClient to use.
     * @param urlString The URL that the GET request should be sent to
     * @return response-code and response(response body) of the HTTP GET in the
     * MAP format. If the response is not 200, the response(response body) will
     * be empty.
     * @throws IOException when fetching data fails.
     */
    public static HttpResponse doGet(final CloseableHttpClient httpClient, String urlString) throws IOException {
        LOGGER.debug("Getting: {}", urlString);
        HttpGet request = new HttpGet(urlString);
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            HttpResponse result = new HttpResponse(response.getStatusLine().getStatusCode());
            if (result.code == 200) {
                result.setResponse(EntityUtils.toString(response.getEntity()));
            } else {
                result.setResponse("");
            }
            return result;
        }
    }

    public static class HttpResponse {

        public final int code;
        public String response;

        public HttpResponse(int code) {
            this.code = code;
        }

        public HttpResponse(int code, String response) {
            this.code = code;
            this.response = response;
        }

        public void setResponse(String response) {
            this.response = response;
        }

        @Override
        public String toString() {
            return "HttpResponse: " + code + " " + response;
        }
    }

}
