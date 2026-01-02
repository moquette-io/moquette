package io.moquette.persistence;

import io.netty.handler.codec.mqtt.MqttProperties;
import org.h2.mvstore.type.StringDataType;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Sets of utility methods used for serialization and deserialization in
 * H2's DataTypes and SegmentedPersistentQueue serdes.
 * */
final class SerdesUtils {
    static boolean containsProperties(ByteBuffer buff) {
        return buff.get() == 1;
    }

    /**
     * Deserialize the array of MqttProperties.
     *
     * @param propertyDecoder function that read bytes from buffer and create a single MQTT property instance.
     * */
    public static MqttProperties.MqttProperty[] readProperties(ByteBuffer buff,
                                                               Function<ByteBuffer, MqttProperties.MqttProperty> propertyDecoder) {
        final int numProperties = buff.getInt();
        final List<MqttProperties.MqttProperty> properties = new ArrayList<>(numProperties);
        for (int i = 0; i < numProperties; i++) {
            MqttProperties.MqttProperty property = propertyDecoder.apply(buff);
            properties.add(property);
        }
        return properties.toArray(new MqttProperties.MqttProperty[0]);
    }

    static MqttProperties.MqttProperty<? extends Serializable> readSingleProperty(ByteBuffer buff,
                                                                                  Function<ByteBuffer, byte[]> bytearrayDecoder) {
        byte propTypeValue = buff.get();
        if (propTypeValue >= PropertyDataType.MqttPropertyEnum.values().length) {
            throw new IllegalStateException("Unrecognized property type value: " + propTypeValue);
        }
        PropertyDataType.MqttPropertyEnum type = PropertyDataType.MqttPropertyEnum.values()[propTypeValue];

        int propertyId = buff.getInt();
        switch (type) {
            case STRING:
                String value = StringDataType.INSTANCE.read(buff);
                return new MqttProperties.StringProperty(propertyId, value);
            case INTEGER:
                return new MqttProperties.IntegerProperty(propertyId, buff.getInt());
            case BINARY:
                return new MqttProperties.BinaryProperty(propertyId, bytearrayDecoder.apply(buff));
            default:
                throw new IllegalStateException("Unrecognized property type value: " + propTypeValue);
        }
    }
}
