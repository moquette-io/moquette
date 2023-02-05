package io.moquette.persistence;

import io.moquette.broker.AbstractSessionMessageQueue;
import io.moquette.broker.SessionRegistry;
import io.moquette.broker.subscriptions.Topic;
import io.moquette.broker.unsafequeues.Queue;
import io.moquette.broker.unsafequeues.QueueException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class SegmentPersistentQueue extends AbstractSessionMessageQueue<SessionRegistry.EnqueuedMessage> {

    private static class SerDes {

        private enum MessageType {PUB_REL_MARKER, PUBLISHED_MESSAGE}

        public ByteBuffer toBytes(SessionRegistry.EnqueuedMessage message) {
            final int memorySize = getMemory(message);
            final ByteBuffer payload = ByteBuffer.allocate(memorySize);
            payload.mark();
            write(message, payload);
            payload.reset();
            return payload;
        }

        private void write(SessionRegistry.EnqueuedMessage obj, ByteBuffer buff) {
            if (obj instanceof SessionRegistry.PublishedMessage) {
                buff.put((byte) MessageType.PUBLISHED_MESSAGE.ordinal());

                final SessionRegistry.PublishedMessage casted = (SessionRegistry.PublishedMessage) obj;
                buff.put((byte) casted.getPublishingQos().value());

                final String topic = casted.getTopic().toString();

                writeTopic(buff, topic);
                writePayload(buff, casted.getPayload());
            } else if (obj instanceof SessionRegistry.PubRelMarker) {
                buff.put((byte) MessageType.PUB_REL_MARKER.ordinal());
            } else {
                throw new IllegalArgumentException("Unrecognized message class " + obj.getClass());
            }
        }

        private void writePayload(ByteBuffer target, ByteBuf source) {
            final int payloadSize = source.readableBytes();
            byte[] rawBytes = new byte[payloadSize];
            source.duplicate().readBytes(rawBytes).release();
            target.putInt(payloadSize);
            target.put(rawBytes);
        }

        private void writeTopic(ByteBuffer buff, String topic) {
            final byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
            buff.putInt(topicBytes.length).put(topicBytes);
        }

        private int getMemory(SessionRegistry.EnqueuedMessage obj) {
            if (obj instanceof SessionRegistry.PubRelMarker) {
                return 1;
            }
            final SessionRegistry.PublishedMessage casted = (SessionRegistry.PublishedMessage) obj;
            return 1 + // message type
                1 + // qos
                topicMemorySize(casted.getTopic()) +
                payloadMemorySize(casted.getPayload());
        }

        private int payloadMemorySize(ByteBuf payload) {
            return 4 + // size
                payload.readableBytes();
        }

        private int topicMemorySize(Topic topic) {
            return 4 + // size
                topic.toString().getBytes(StandardCharsets.UTF_8).length;
        }

        public SessionRegistry.EnqueuedMessage fromBytes(ByteBuffer buff) {
            final byte messageType = buff.get();
            if (messageType == MessageType.PUB_REL_MARKER.ordinal()) {
                return new SessionRegistry.PubRelMarker();
            } else if (messageType == MessageType.PUBLISHED_MESSAGE.ordinal()) {
                final MqttQoS qos = MqttQoS.valueOf(buff.get());
                final String topicStr = readTopic(buff);
                final ByteBuf payload = readPayload(buff);
                return new SessionRegistry.PublishedMessage(Topic.asTopic(topicStr), qos, payload, false);
            } else {
                throw new IllegalArgumentException("Can't recognize record of type: " + messageType);
            }
        }

        private String readTopic(ByteBuffer buff) {
            final int stringLen = buff.getInt();
            final byte[] rawString = new byte[stringLen];
            buff.get(rawString);
            return new String(rawString, StandardCharsets.UTF_8);
        }

        private ByteBuf readPayload(ByteBuffer buff) {
            final int payloadSize = buff.getInt();
            byte[] payload = new byte[payloadSize];
            buff.get(payload);
            return Unpooled.wrappedBuffer(payload);
        }
    }

    private final Queue segmentedQueue;
    private final SerDes serdes = new SerDes();

    public SegmentPersistentQueue(Queue segmentedQueue) {
        this.segmentedQueue = segmentedQueue;
    }

    @Override
    public void enqueue(SessionRegistry.EnqueuedMessage message) {
        checkEnqueuePreconditions(message);

        final ByteBuffer payload = serdes.toBytes(message);
        try {
            segmentedQueue.enqueue(payload);
        } catch (QueueException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SessionRegistry.EnqueuedMessage dequeue() {
        checkDequeuePreconditions();

        final Optional<ByteBuffer> dequeue;
        try {
            dequeue = segmentedQueue.dequeue();
        } catch (QueueException e) {
            throw new RuntimeException(e);
        }
        if (!dequeue.isPresent()) {
            return null;
        }

        final ByteBuffer content = dequeue.get();
        return serdes.fromBytes(content);
    }

    @Override
    public boolean isEmpty() {
        return segmentedQueue.isEmpty();
    }

    @Override
    public void closeAndPurge() {
        closed = true;
    }
}
