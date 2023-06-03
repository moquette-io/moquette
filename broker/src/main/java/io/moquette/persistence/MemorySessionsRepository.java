package io.moquette.persistence;

import io.moquette.broker.ISessionsRepository;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MemorySessionsRepository implements ISessionsRepository {

    private final ConcurrentMap<String, SessionData> sessions = new ConcurrentHashMap<>();

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
}
