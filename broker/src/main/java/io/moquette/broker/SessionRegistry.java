package io.moquette.broker;

import io.netty.handler.codec.mqtt.MqttConnectMessage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class SessionRegistry {

    private final ConcurrentMap<String, Session> pool = new ConcurrentHashMap<>();

    void bindToSession(MQTTConnection mqttConnection, MqttConnectMessage msg) {
        final String clientId = msg.payload().clientIdentifier();
        if (!pool.containsKey(clientId)) {
            final boolean success = createNewSession(mqttConnection, msg);
            if (!success) {
                final boolean newIsClean = msg.variableHeader().isCleanSession();
                final Session oldSession = pool.get(clientId);
                if (newIsClean && oldSession.disconnected()) {
                    createNewSessionRemovingTheDisconnected()
                }
            }
        }

    }

    /*
     * Create new session, and try to put in the pool. Return false if in the mean time other client connected with
     * same ID.
     * */
    private boolean createNewSession(MQTTConnection mqttConnection, MqttConnectMessage msg) {
        final String clientId = msg.payload().clientIdentifier();

        final boolean clean = msg.variableHeader().isCleanSession();
        final Session.Will will = createWill(msg);

        final Session newSession = new Session(clientId, clean, will);
        newSession.markConnected();
        newSession.bind(mqttConnection);

        final Session previous = pool.putIfAbsent(clientId, newSession);
        return previous == null;
    }

    private Session.Will createWill(MqttConnectMessage msg) {
        final byte[] willPayload = msg.payload().willMessageInBytes();
        final String willTopic = msg.payload().willTopic();
        return new Session.Will(willTopic, willPayload);
    }
}
