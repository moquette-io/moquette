package io.moquette.persistence;

import io.moquette.broker.ISubscriptionsRepository;
import io.moquette.broker.Utils;
import io.moquette.broker.subscriptions.ShareName;
import io.moquette.broker.subscriptions.SharedSubscription;
import io.moquette.broker.subscriptions.Subscription;
import io.moquette.broker.subscriptions.SubscriptionIdentifier;
import io.moquette.broker.subscriptions.Topic;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;
import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.BasicDataType;
import org.h2.mvstore.type.StringDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class H2SubscriptionsRepository implements ISubscriptionsRepository {

    private static final Logger LOG = LoggerFactory.getLogger(H2SubscriptionsRepository.class);
    private static final String SUBSCRIPTIONS_MAP = "subscriptions";
    private static final String SHARED_SUBSCRIPTIONS_MAP = "shared_subscriptions";
    private final MVStore mvStore;
    private final MVMap.Builder<Utils.Couple<ShareName, Topic>, SubscriptionOptionAndId> submapBuilder;

    private MVMap<String, Subscription> subscriptions;
    // clientId -> shared subscription map name
    private MVMap<String, String> sharedSubscriptions;
    private final MVMap.Builder<String, Subscription> subscriptionBuilder = new MVMap.Builder<String, Subscription>()
        .valueType(new SubscriptionValueType());

    H2SubscriptionsRepository(MVStore mvStore) {
        this.mvStore = mvStore;

        submapBuilder = new MVMap.Builder<Utils.Couple<ShareName, Topic>, SubscriptionOptionAndId>()
            .keyType(new CoupleValueType())
            .valueType(new SubscriptionOptionAndIdValueType());

        this.subscriptions = mvStore.openMap(SUBSCRIPTIONS_MAP, subscriptionBuilder);
        sharedSubscriptions = mvStore.openMap(SHARED_SUBSCRIPTIONS_MAP);
    }

    @Override
    public Set<Subscription> listAllSubscriptions() {
        LOG.debug("Retrieving existing subscriptions");

        Set<Subscription> results = new HashSet<>();
        Cursor<String, Subscription> mapCursor = subscriptions.cursor(null);
        while (mapCursor.hasNext()) {
            String subscriptionStr = mapCursor.next();
            results.add(mapCursor.getValue());
        }
        LOG.debug("Loaded {} subscriptions", results.size());
        return results;
    }

    @Override
    public void addNewSubscription(Subscription subscription) {
        subscriptions.put(subscription.getTopicFilter() + "-" + subscription.getClientId(), subscription);
    }

    @Override
    public void removeSubscription(String topicFilter, String clientID) {
        subscriptions.remove(topicFilter + "-" + clientID);
    }

    @Override
    public void removeAllSharedSubscriptions(String clientId) {
        final String sharedSubsMapName = sharedSubscriptions.get(clientId);
        if (sharedSubsMapName == null) {
            LOG.debug("Removing all shared subscription of a non existing client: {}", clientId);
            return;
        }
        wipeAllSharedSubscripptions(clientId, sharedSubsMapName);
    }

    private void wipeAllSharedSubscripptions(String clientId, String sharedSubsMapName) {
        mvStore.removeMap(sharedSubsMapName);
        sharedSubscriptions.remove(clientId);
    }

    @Override
    public void removeSharedSubscription(String clientId, ShareName share, Topic topicFilter) {
        final String sharedSubsMapName = sharedSubscriptions.get(clientId);
        if (sharedSubsMapName == null) {
            LOG.info("Removing a non existing shared subscription for client: {}", clientId);
            return;
        }
        MVMap<Utils.Couple<ShareName, Topic>, SubscriptionOptionAndId> subMap = mvStore.openMap(sharedSubsMapName, submapBuilder);
        Utils.Couple<ShareName, Topic> sharedSubKey = Utils.Couple.of(share, topicFilter);

        // remove from submap, null means the key didn't exist
        if (subMap.remove(sharedSubKey) == null) {
            LOG.info("Removing non existing shared subscription name: {} filter: {} for client: {}", share, topicFilter, clientId);
            return;
        }
        // if the submap is empty, clean up all the things
        if (subMap.isEmpty()) {
            LOG.debug("Removing all references for share subscription clientId: {} share: {} filter: {}", clientId, share, topicFilter);
            wipeAllSharedSubscripptions(clientId, sharedSubsMapName);
        }
    }

    private static class SubscriptionOptionAndId implements Serializable {
        final MqttSubscriptionOption option;
        final Integer subscriptionIdentifier;

        public SubscriptionOptionAndId(MqttSubscriptionOption option, int subscriptionIdentifier) {
            this.option = option;
            this.subscriptionIdentifier = subscriptionIdentifier;
        }

        public SubscriptionOptionAndId(MqttSubscriptionOption option) {
            this.option = option;
            this.subscriptionIdentifier = null;
        }
    }

    @Override
    public void addNewSharedSubscription(String clientId, ShareName share, Topic topicFilter, MqttSubscriptionOption option) {
        SubscriptionOptionAndId qosPart = new SubscriptionOptionAndId(option);
        storeNewSharedSubscription(clientId, share, topicFilter, qosPart);
    }

    private void storeNewSharedSubscription(String clientId, ShareName share, Topic topicFilter, SubscriptionOptionAndId value) {
        String sharedSubsMapName = sharedSubscriptions.computeIfAbsent(clientId,
            H2SubscriptionsRepository::computeShareSubscriptionSubMap);

        // maps the couple (share name, topic) to requested qos
        MVMap<Utils.Couple<ShareName, Topic>, SubscriptionOptionAndId> subMap = mvStore.openMap(sharedSubsMapName, submapBuilder);
        subMap.put(Utils.Couple.of(share, topicFilter), value);
    }

    @Override
    public void addNewSharedSubscription(String clientId, ShareName share, Topic topicFilter, MqttSubscriptionOption option,
                                         SubscriptionIdentifier subscriptionIdentifier) {
        SubscriptionOptionAndId qosAndSubscriptionIdPart = new SubscriptionOptionAndId(option, subscriptionIdentifier.value());
        storeNewSharedSubscription(clientId, share, topicFilter, qosAndSubscriptionIdPart);
    }

    @Override
    public Collection<SharedSubscription> listAllSharedSubscription() {
        List<SharedSubscription> result = new ArrayList<>();

        for (Map.Entry<String, String> entry : sharedSubscriptions.entrySet()) {
            String clientId = entry.getKey();
            String sharedSubsMapName = entry.getValue();

            MVMap<Utils.Couple<ShareName, Topic>, SubscriptionOptionAndId> subMap = mvStore.openMap(sharedSubsMapName, submapBuilder);
            for (Map.Entry<Utils.Couple<ShareName, Topic>, SubscriptionOptionAndId> subEntry : subMap.entrySet()) {
                final ShareName shareName = subEntry.getKey().v1;
                final Topic topicFilter = subEntry.getKey().v2;
                final MqttSubscriptionOption option = subEntry.getValue().option;
                SharedSubscription subscription;
                if (subEntry.getValue().subscriptionIdentifier == null) {
                    // without subscription identifier
                    subscription = new SharedSubscription(shareName, topicFilter, clientId, option);
                } else {
                    // with subscription identifier
                    SubscriptionIdentifier subscriptionId = new SubscriptionIdentifier(subEntry.getValue().subscriptionIdentifier);
                    subscription = new SharedSubscription(shareName, topicFilter, clientId, option, subscriptionId);
                }
                result.add(subscription);
            }
        }

        return result;
    }

    private static String computeShareSubscriptionSubMap(String sessionId) {
        return SHARED_SUBSCRIPTIONS_MAP + "_" + sessionId;
    }

    static final class CoupleValueType extends BasicDataType<Utils.Couple<ShareName, Topic>> {

        private final Comparator<Utils.Couple<ShareName, Topic>> coupleComparator =
            Comparator.<Utils.Couple<ShareName, Topic>, String>comparing(c -> c.v1.getShareName())
            .thenComparing(c -> c.v2.toString());

        @Override
        public int compare(Utils.Couple<ShareName, Topic> var1, Utils.Couple<ShareName, Topic> var2) {
            return coupleComparator.compare(var1, var2);
        }

        @Override
        public int getMemory(Utils.Couple<ShareName, Topic> couple) {
            return StringDataType.INSTANCE.getMemory(couple.v1.getShareName()) +
                   StringDataType.INSTANCE.getMemory(couple.v2.toString());
        }

        @Override
        public void write(WriteBuffer buff, Utils.Couple<ShareName, Topic> couple) {
            StringDataType.INSTANCE.write(buff, couple.v1.getShareName());
            StringDataType.INSTANCE.write(buff, couple.v2.toString());
        }

        @Override
        public Utils.Couple<ShareName, Topic> read(ByteBuffer buffer) {
            String shareName = StringDataType.INSTANCE.read(buffer);
            String topicFilter = StringDataType.INSTANCE.read(buffer);
            return new Utils.Couple<>(new ShareName(shareName), Topic.asTopic(topicFilter));
        }

        @Override
        public Utils.Couple<ShareName, Topic>[] createStorage(int i) {
            return new Utils.Couple[i];
        }
    }

    private static final class SubscriptionOptionAndIdValueType extends BasicDataType<SubscriptionOptionAndId> {

        @Override
        public int getMemory(SubscriptionOptionAndId obj) {
            return 4 + // integer, subscription identifier
                SubscriptionOptionValueType.INSTANCE.getMemory(obj.option);
        }

        @Override
        public void write(WriteBuffer buff, SubscriptionOptionAndId obj) {
            if (obj.subscriptionIdentifier != null) {
                buff.putInt(obj.subscriptionIdentifier.intValue());
            } else {
                buff.putInt(-1);
            }
            SubscriptionOptionValueType.INSTANCE.write(buff, obj.option);
        }

        @Override
        public SubscriptionOptionAndId read(ByteBuffer buff) {
            int subId = buff.getInt();
            MqttSubscriptionOption option = SubscriptionOptionValueType.INSTANCE.read(buff);
            if (subId != -1) {
                return new SubscriptionOptionAndId(option, subId);
            } else {
                return new SubscriptionOptionAndId(option);
            }
        }

        @Override
        public SubscriptionOptionAndId[] createStorage(int size) {
            return new SubscriptionOptionAndId[size];
        }
    }

    private static final class SubscriptionOptionValueType extends BasicDataType<MqttSubscriptionOption> {
        public static final SubscriptionOptionValueType INSTANCE = new SubscriptionOptionValueType();

        @Override
        public int getMemory(MqttSubscriptionOption obj) {
            return 1;
        }

        @Override
        public void write(WriteBuffer buff, MqttSubscriptionOption opt) {
            // 2 bits for QoS (LSB)
            // 1 flag for no local
            // 1 flag for retains as published
            // 2 bits for retains handling policy (MSB)
            byte composed = (byte) (opt.qos().value() & 0x03); // qos
            composed = (byte) (composed | ((byte)(opt.isNoLocal() ? 1 : 0) << 2)); // no local
            composed = (byte) (composed | ((byte)(opt.isRetainAsPublished() ? 1 : 0) << 3)); // retains as published
            composed = (byte) (composed | (byte) (opt.retainHandling().value() << 4));
            buff.put(composed);
        }

        @Override
        public MqttSubscriptionOption read(ByteBuffer buff) {
            byte fields = buff.get();
            final MqttQoS qos = MqttQoS.valueOf(fields & 0x03);
            final boolean noLocal = (fields & 0x04) > 0;
            final boolean retainAsPublished = (fields & 0x08) > 0;
            final MqttSubscriptionOption.RetainedHandlingPolicy retainedHandlingPolicy =
                MqttSubscriptionOption.RetainedHandlingPolicy.valueOf((fields & 0x30) >> 4);

            return new MqttSubscriptionOption(qos, noLocal, retainAsPublished, retainedHandlingPolicy);
        }

        @Override
        public MqttSubscriptionOption[] createStorage(int size) {
            return new MqttSubscriptionOption[size];
        }
    }


    private static final class SubscriptionValueType extends BasicDataType<Subscription> {

        @Override
        public int getMemory(Subscription sub) {
            return StringDataType.INSTANCE.getMemory(sub.getClientId()) +
                StringDataType.INSTANCE.getMemory(sub.getTopicFilter().toString()) +
                SubscriptionOptionValueType.INSTANCE.getMemory(sub.option()) +
                1 + // flag to say if share name is present and/or subscription identifier
                (sub.hasShareName() ? StringDataType.INSTANCE.getMemory(sub.getShareName()) : 0) +
                (sub.hasSubscriptionIdentifier() ? 4 : 0);
        }

        @Override
        public void write(WriteBuffer buff, Subscription sub) {
            StringDataType.INSTANCE.write(buff, sub.getClientId());
            StringDataType.INSTANCE.write(buff, sub.getTopicFilter().toString());
            SubscriptionOptionValueType.INSTANCE.write(buff, sub.option());
            final byte flag = (byte) ((sub.hasShareName() ? 0x1 : 0x0) |
                              (sub.hasSubscriptionIdentifier() ? 0x2 : 0x0));
            buff.put(flag);
            if (sub.hasShareName()) {
                StringDataType.INSTANCE.write(buff, sub.getShareName());
            }
            if (sub.hasSubscriptionIdentifier()) {
                buff.putInt(sub.getSubscriptionIdentifier().value());
            }
        }

        @Override
        public Subscription read(ByteBuffer buff) {
            final String clientId = StringDataType.INSTANCE.read(buff);
            final String topicFilter = StringDataType.INSTANCE.read(buff);
            final MqttSubscriptionOption options = SubscriptionOptionValueType.INSTANCE.read(buff);
            byte flag = buff.get();
            boolean hasShareName = (flag & (byte) 0x1) > 0;
            boolean hasSubscriptionIdentifier = (flag & (byte) 0x2) > 0;

            if (hasShareName) {
                String shareName = StringDataType.INSTANCE.read(buff);
                if (hasSubscriptionIdentifier) {
                    SubscriptionIdentifier subId = new SubscriptionIdentifier(buff.getInt());
                    return new Subscription(clientId, Topic.asTopic(topicFilter), options, shareName, subId);
                } else {
                    return new Subscription(clientId, Topic.asTopic(topicFilter), options, shareName);
                }
            } else {
                if (hasSubscriptionIdentifier) {
                    SubscriptionIdentifier subId = new SubscriptionIdentifier(buff.getInt());
                    return new Subscription(clientId, Topic.asTopic(topicFilter), options, subId);
                } else {
                    return new Subscription(clientId, Topic.asTopic(topicFilter), options);
                }
            }
        }

        @Override
        public Subscription[] createStorage(int size) {
            return new Subscription[size];
        }
    }
}
