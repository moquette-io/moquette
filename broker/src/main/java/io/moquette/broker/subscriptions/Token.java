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

package io.moquette.broker.subscriptions;

/**
 * Internal use only class.
 */
public class Token implements Comparable<Token> {

    static final Token EMPTY = new Token("");
    static final Token MULTI = new Token("#");
    static final Token SINGLE = new Token("+");
    final String name;
    boolean lastSubToken;

    protected Token(String s) {
        this(s, true);
    }

    protected Token(String s, boolean isLastSub) {
        name = s;
        lastSubToken = isLastSub;
    }

    protected String name() {
        return name;
    }

    protected void setLastSubToken(boolean lastSubToken) {
        this.lastSubToken = lastSubToken;
    }

    protected boolean isLastSubToken() {
        return lastSubToken;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 29 * hash + (this.name != null ? this.name.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Token other = (Token) obj;
        if ((this.name == null) ? (other.name != null) : !this.name.equals(other.name)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public int compareTo(Token other) {
        if (name == null) {
            if (other.name == null) {
                return 0;
            }
            return 1;
        }
        if (other.name == null) {
            return -1;
        }
        return name.compareTo(other.name);
    }

    /**
     * Token which starts with $ is reserved
     * */
    public boolean isReserved() {
        return name.startsWith("$");
    }
}
