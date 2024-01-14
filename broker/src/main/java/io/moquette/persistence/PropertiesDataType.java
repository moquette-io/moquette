package io.moquette.persistence;

import io.netty.handler.codec.mqtt.MqttProperties;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.BasicDataType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

final class PropertiesDataType extends BasicDataType<MqttProperties.MqttProperty[]> {

    private final PropertyDataType propertyDataType = new PropertyDataType();

    @Override
    public int getMemory(MqttProperties.MqttProperty[] obj) {
        return 4 + // integer containing the number of properties
            Arrays.stream(obj).mapToInt(propertyDataType::getMemory).sum();
    }

    @Override
    public void write(WriteBuffer buff, MqttProperties.MqttProperty[] obj) {
        // store property list size
        buff.putInt(obj.length);
        for (MqttProperties.MqttProperty property : obj) {
            propertyDataType.write(buff, property);
        }
    }

    @Override
    public MqttProperties.MqttProperty[] read(ByteBuffer buff) {
        final int numProperties = buff.getInt();
        final List<MqttProperties.MqttProperty> properties = new ArrayList<>(numProperties);
        for (int i = 0; i < numProperties; i++) {
            MqttProperties.MqttProperty property = propertyDataType.read(buff);
            properties.add(property);
        }
        return properties.toArray(new MqttProperties.MqttProperty[0]);
    }

    @Override
    public MqttProperties.MqttProperty[][] createStorage(int size) {
        return new MqttProperties.MqttProperty[size][];
    }
}
