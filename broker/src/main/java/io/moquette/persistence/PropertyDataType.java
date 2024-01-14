package io.moquette.persistence;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttProperties;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.BasicDataType;
import org.h2.mvstore.type.StringDataType;

import java.nio.ByteBuffer;

class PropertyDataType extends BasicDataType<MqttProperties.MqttProperty> {
    private enum MqttPropertyEnum {STRING, INTEGER, BINARY}

    private final ByteBufDataType binaryDataType = new ByteBufDataType();

    @Override
    public int getMemory(MqttProperties.MqttProperty property) {
        int propSize = 4; // propertyId
        if (property instanceof MqttProperties.StringProperty) {
            MqttProperties.StringProperty stringProp = (MqttProperties.StringProperty) property;
            propSize += StringDataType.INSTANCE.getMemory(stringProp.value());
        } else if (property instanceof MqttProperties.IntegerProperty) {
            propSize += 4; // integer is 4 bytes
        } else if (property instanceof MqttProperties.BinaryProperty) {
            MqttProperties.BinaryProperty byteArrayProp = (MqttProperties.BinaryProperty) property;
            propSize += binaryDataType.getMemory(Unpooled.wrappedBuffer(byteArrayProp.value()));
        }
        return 1 + // property type
            propSize;
    }

    @Override
    public void write(WriteBuffer buff, MqttProperties.MqttProperty property) {
        if (property instanceof MqttProperties.StringProperty) {
            MqttProperties.StringProperty stringProp = (MqttProperties.StringProperty) property;
            writePropertyType(buff, MqttPropertyEnum.STRING);
            buff.putInt(stringProp.propertyId());
            StringDataType.INSTANCE.write(buff, stringProp.value());
        } else if (property instanceof MqttProperties.IntegerProperty) {
            MqttProperties.IntegerProperty intProp = (MqttProperties.IntegerProperty) property;
            writePropertyType(buff, MqttPropertyEnum.INTEGER);
            buff.putInt(intProp.propertyId());
            buff.putInt(intProp.value());
        } else if (property instanceof MqttProperties.BinaryProperty) {
            MqttProperties.BinaryProperty byteArrayProp = (MqttProperties.BinaryProperty) property;
            writePropertyType(buff, MqttPropertyEnum.BINARY);
            binaryDataType.write(buff, Unpooled.wrappedBuffer(byteArrayProp.value()));
        }
        // TODO UserProperties and UserProperty?
    }

    private static void writePropertyType(WriteBuffer buff, MqttPropertyEnum mqttPropertyEnum) {
        buff.put((byte) mqttPropertyEnum.ordinal());
    }

    @Override
    public MqttProperties.MqttProperty read(ByteBuffer buff) {
        byte propTypeValue = buff.get();
        if (propTypeValue >= MqttPropertyEnum.values().length) {
            throw new IllegalStateException("Unrecognized property type value: " + propTypeValue);
        }
        MqttPropertyEnum type = MqttPropertyEnum.values()[propTypeValue];

        int propertyId = buff.getInt();
        switch (type) {
            case STRING:
                String value = StringDataType.INSTANCE.read(buff);
                return new MqttProperties.StringProperty(propertyId, value);
            case INTEGER:
                return new MqttProperties.IntegerProperty(propertyId, buff.getInt());
            case BINARY:
                ByteBuf byteArray = binaryDataType.read(buff);
                return new MqttProperties.BinaryProperty(propertyId, byteArray.array());
            default:
                throw new IllegalStateException("Unrecognized property type value: " + propTypeValue);
        }
    }

    @Override
    public MqttProperties.MqttProperty[] createStorage(int size) {
        return new MqttProperties.MqttProperty[size];
    }
}
