package io.moquette.broker;

class Session {

    enum SessionStatus {
        CONNECTED
    }

    static final class Will {

        private final String topic;
        private final byte[] payload;

        Will(String topic, byte[] payload) {
            this.topic = topic;
            this.payload = payload;
        }
    }

    private final String clientId;
    private final boolean clean;
    private final Will will;
    private SessionStatus status;
    private MQTTConnection mqttConnection;

    Session(String clientId, boolean clean, Will will) {
        this.clientId = clientId;
        this.clean = clean;
        this.will = will;
    }

    void markConnected() {
        status = SessionStatus.CONNECTED;
    }

    void bind(MQTTConnection mqttConnection) {
        this.mqttConnection = mqttConnection;
    }
}
