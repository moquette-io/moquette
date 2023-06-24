package io.moquette.broker.security;

import io.moquette.broker.ClientDescriptor;

public interface IConnectionFilter {
    boolean allowConnection(ClientDescriptor clientDescriptor);
}
