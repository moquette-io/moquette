/*
 * Copyright (c) 2012-2023 The original author or authors
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
package io.moquette.broker.subscriptions;

import java.util.Objects;

/**
 * Shared subscription's name.
 */
// It's public because used by PostOffice
public final class ShareName {
    private final String shareName;

    public ShareName(String shareName) {
        this.shareName = shareName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (o instanceof String) {
            return Objects.equals(shareName, (String) o);
        }
        if (getClass() != o.getClass()) return false;
        ShareName shareName1 = (ShareName) o;
        return Objects.equals(shareName, shareName1.shareName);
    }

    public String getShareName() {
        return shareName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(shareName);
    }

    @Override
    public String toString() {
        return "ShareName{" +
            "shareName='" + shareName + '\'' +
            '}';
    }
}
