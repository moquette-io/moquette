package io.moquette.broker;

import io.moquette.broker.security.IConnectionFilter;

import java.util.Set;
import java.util.HashSet;
import java.util.stream.Stream;

public class MockConnectionFilter implements IConnectionFilter {
    private Set<String> bannedClientIds = new HashSet<>();
    private Set<String> bannedAddresses = new HashSet<>();
    @Override
    public boolean allowConnection(ClientDescriptor clientDescriptor) {
        return !bannedClientIds.contains(clientDescriptor.getClientID())
            && !bannedAddresses.contains(clientDescriptor.getAddress());
    }

    public MockConnectionFilter banClientId(String clientId) {
        bannedClientIds.add(clientId);
        return this;
    }

    public MockConnectionFilter banAddress(String address) {
        bannedAddresses.add(address);
        return this;
    }

    public MockConnectionFilter reset() {
        bannedClientIds = new HashSet<>();
        bannedAddresses = new HashSet<>();
        return this;
    }
}
