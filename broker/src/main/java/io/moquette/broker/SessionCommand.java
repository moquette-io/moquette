package io.moquette.broker;

import io.netty.handler.codec.mqtt.MqttConnectMessage;

abstract class SessionCommand {

    enum CommandType { CONNECT, SUBSCRIBE, UNSUBSCRIBE, PUBLISH, DISCONNECT;}

    private final String sessionId;
    private final CommandType type;

    private  SessionCommand(String sessionId, CommandType type) {
        this.sessionId = sessionId;
        this.type = type;
    }

    public CommandType getType() {
        return this.type;
    }

    public String getSessionId() {
        return this.sessionId;
    }

    public abstract void execute();

    static final class Connect extends SessionCommand {
        private final MQTTConnection mqttConnection;
        private final MqttConnectMessage msg;

        public Connect(String sessionId, MQTTConnection mqttConnection, MqttConnectMessage msg) {
            super(sessionId, CommandType.CONNECT);
            this.mqttConnection = mqttConnection;
            this.msg = msg;
        }

        @Override
        public void execute() {
            mqttConnection.executeConnect(this.msg, getSessionId());
        }
    }
}
