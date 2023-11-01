package io.moquette.persistence;

import io.moquette.broker.ISessionsRepository;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

public class MemorySessionsRepository implements ISessionsRepository {

    private final ConcurrentMap<String, SessionData> sessions = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Will> wills = new ConcurrentHashMap<>();

    @Override
    public Collection<SessionData> list() {
        return sessions.values();
    }

    @Override
    public void saveSession(SessionData session) {
        sessions.put(session.clientId(), session);
    }

    @Override
    public void delete(SessionData session) {
        sessions.remove(session.clientId());
    }

    @Override
    public void listSessionsWill(BiConsumer<String, Will> visitor) {
        wills.entrySet().stream()
            .forEach(e -> visitor.accept(e.getKey(), e.getValue()));
    }

    @Override
    public void saveWill(String clientId, Will will) {
        wills.put(clientId, will);
    }

    @Override
    public void deleteWill(String clientId) {
        wills.remove(clientId);
    }
}
