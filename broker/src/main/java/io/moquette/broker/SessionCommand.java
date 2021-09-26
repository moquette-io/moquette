package io.moquette.broker;

import io.netty.handler.codec.mqtt.MqttConnectMessage;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

abstract class SessionCommand {

    enum CommandType { CONNECT, SUBSCRIBE, UNSUBSCRIBE, PUBLISH, DISCONNECT, PUBACK, PUBREC, PUBREL, PUBCOMP}

    private final String sessionId;

    private final CommandType type;
    private final CompletableFuture<Void> task;
    private  SessionCommand(String sessionId, CommandType type) {
        this.sessionId = sessionId;
        this.type = type;
        this.task = new CompletableFuture<>();
    }

    public CommandType getType() {
        return this.type;
    }

    public String getSessionId() {
        return this.sessionId;
    }

    public abstract void execute() throws Exception;

    public CompletableFuture<Void> complete() {
        task.complete(null);
        return this.task;
    }

    public CompletableFuture<Void> completableFuture() {
        return task;
    }

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

    static final class Disconnect extends SessionCommand {

        private final MQTTConnection mqttConnection;

        public Disconnect(String sessionId, MQTTConnection mqttConnection) {
            super(sessionId, CommandType.DISCONNECT);
            this.mqttConnection = mqttConnection;
        }

        @Override
        public void execute() {
            mqttConnection.executeDisconnect(getSessionId());
        }
    }

    static final class Publish extends SessionCommand {

        private final Callable<Void> action;

        public Publish(String sessionId, Callable<Void> action) {
            super(sessionId, CommandType.PUBLISH);
            this.action = action;
        }

        @Override
        public void execute() throws Exception {
            action.call();
        }
    }
}
