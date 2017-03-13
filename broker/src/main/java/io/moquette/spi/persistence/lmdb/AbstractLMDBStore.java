package io.moquette.spi.persistence.lmdb;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.lmdbjava.Env;

public abstract class AbstractLMDBStore {

    protected final Env<ByteBuffer> env;

    protected AbstractLMDBStore(Env<ByteBuffer> env) {
        super();
        this.env = env;
    }
    
    protected ByteBuffer createValue(String stringValue) {
        final ByteBuffer val = allocateDirect(700);
        val.put(stringValue.getBytes(UTF_8)).flip();
        return val;
    }

    protected ByteBuffer createValue(int intValue) {
        ByteBuffer val = allocateDirect(4);
        val.putInt(intValue).flip();
        return val;
    }
    
    protected ByteBuffer createValue(Serializable javaValue) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);) {
            oos.writeObject(javaValue);
            byte[] byteArray = bos.toByteArray();            
            ByteBuffer byteBuffer = allocateDirect(byteArray.length);
            byteBuffer.put(byteArray).flip();
            return byteBuffer;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    protected ByteBuffer createKey(String keyString) {
        final ByteBuffer key = allocateDirect(env.getMaxKeySize());
        key.put(keyString.getBytes(UTF_8)).flip();
        return key;
    }

    protected ByteBuffer createClientMessageKey(String clientID, int messageID) {
        final ByteBuffer key = allocateDirect(env.getMaxKeySize());
        key.put(clientID.getBytes(UTF_8)).flip();
        key.putInt(messageID);
        return key;
    }
    
    protected <T extends Serializable> T decodeValue(ByteBuffer fetchedVal) {
        try (ByteBufferBackedInputStream is = new ByteBufferBackedInputStream(fetchedVal);
                ObjectInputStream ois = new ObjectInputStream(is);) {
            return (T) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    protected String decodeString(ByteBuffer fetchedVal) {
        return UTF_8.decode(fetchedVal).toString();
    }
    
    protected int decodeInt(ByteBuffer fetchedVal) {
        return fetchedVal.getInt();
    }
    
    protected Env<ByteBuffer> getEnv() {
        return env;
    }
    
}
