package io.moquette.broker;

import io.netty.handler.codec.mqtt.MqttProperties;

import java.util.function.Consumer;

public class MessageBuilders {

    static MqttProperties withConnAckProps(Consumer<ConnAckPropertiesBuilder> consumer) {
        final ConnAckPropertiesBuilder propsBuilder = new ConnAckPropertiesBuilder();
        consumer.accept(propsBuilder);
        return propsBuilder.build();

    }

    static class ConnAckPropertiesBuilder {

        private String clientId;
        private Long sessionExpiryInterval;
        private int receiveMaximum = 0;
        private Byte maximumQos;
        private boolean retain = false;
        private Long maximumPacketSize;
        private int topicAliasMaximum = 0;
        private String reasonString;
        private MqttProperties.UserProperties userProperties = new MqttProperties.UserProperties();
        private Boolean wildcardSubscriptionAvailable;
        private Boolean subscriptionIdentifiersAvailable;
        private Boolean sharedSubscriptionAvailable;
        private Integer serverKeepAlive;
        private String responseInformation;
        private String serverReference;
        private String authenticationMethod;
        private byte[] authenticationData;

        public MqttProperties build() {
            final MqttProperties props = new MqttProperties();
            if (clientId != null) {
                props.add(new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.ASSIGNED_CLIENT_IDENTIFIER.value(), clientId));
            }
            if (sessionExpiryInterval != null) {
                props.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value(), sessionExpiryInterval.intValue()));
            }
            if (receiveMaximum > 0) {
                props.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM.value(), receiveMaximum));
            }
            if (maximumQos != null) {
                props.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.MAXIMUM_QOS.value(), receiveMaximum));
            }
            props.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.RETAIN_AVAILABLE.value(), retain ? 1 : 0));
            if (maximumPacketSize != null) {
                props.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.MAXIMUM_PACKET_SIZE.value(), maximumPacketSize.intValue()));
            }
            props.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.TOPIC_ALIAS_MAXIMUM.value(), topicAliasMaximum));
            if (reasonString != null) {
                props.add(new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(), reasonString));
            }
            props.add(userProperties);
            if (wildcardSubscriptionAvailable != null) {
                props.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.WILDCARD_SUBSCRIPTION_AVAILABLE.value(), wildcardSubscriptionAvailable ? 1 : 0));
            }
            if (subscriptionIdentifiersAvailable != null) {
                props.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER_AVAILABLE.value(), subscriptionIdentifiersAvailable ? 1 : 0));
            }
            if (sharedSubscriptionAvailable != null) {
                props.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.SHARED_SUBSCRIPTION_AVAILABLE.value(), sharedSubscriptionAvailable ? 1 : 0));
            }
            if (serverKeepAlive != null) {
                props.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.SERVER_KEEP_ALIVE.value(), serverKeepAlive));
            }
            if (responseInformation != null) {
                props.add(new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.RESPONSE_INFORMATION.value(), responseInformation));
            }
            if (serverReference != null) {
                props.add(new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.SERVER_REFERENCE.value(), serverReference));
            }
            if (authenticationMethod != null) {
                props.add(new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.AUTHENTICATION_METHOD.value(), authenticationMethod));
            }
            if (authenticationData != null) {
                props.add(new MqttProperties.BinaryProperty(MqttProperties.MqttPropertyType.AUTHENTICATION_DATA.value(), authenticationData));
            }

            return props;
        }

        public void sessionExpiryInterval(long seconds) {
            this.sessionExpiryInterval = seconds;
        }

        public void receiveMaximum(int value) {
            if (value <= 0) {
                throw new IllegalArgumentException("receive maximum property must be > 0");
            }
            this.receiveMaximum = value;
        }

        public void maximumQos(byte value) {
            if (value != 0 && value != 1) {
                throw new IllegalArgumentException("maximum QoS property could be 0 or 1");
            }
            this.maximumQos = value;
        }

        public void retainAvailable(boolean retain) {
            this.retain = retain;
        }

        public void maximumPacketSize(long size) {
            if (size <= 0) {
                throw new IllegalArgumentException("maximum packet size property must be > 0");
            }
            this.maximumPacketSize = size;
        }

        public void assignedClientId(String clientId) {
            this.clientId = clientId;
        }

        public void topicAliasMaximum(int value) {
            this.topicAliasMaximum = value;
        }

        public void reasonString(String reason) {
            this.reasonString = reason;
        }

        public void userProperty(String name, String value) {
            userProperties.add(name, value);
        }

        public void wildcardSubscriptionAvailable(boolean value) {
            this.wildcardSubscriptionAvailable = value;
        }

        public void subscriptionIdentifiersAvailable(boolean value) {
            this.subscriptionIdentifiersAvailable = value;
        }

        public void sharedSubscriptionAvailable(boolean value) {
            this.sharedSubscriptionAvailable = value;
        }

        public void serverKeepAlive(int seconds) {
            this.serverKeepAlive = seconds;
        }

        public void responseInformation(String value) {
            this.responseInformation = value;
        }

        public void serverReference(String host) {
            this.serverReference = host;
        }

        public void authenticationMethod(String methodName) {
            this.authenticationMethod = methodName;
        }

        public void authenticationData(byte[] rawData) {
            this.authenticationData = rawData;
        }
    }

}
