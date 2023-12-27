package io.moquette.persistence;

import io.moquette.broker.ISubscriptionsRepository;
import io.moquette.broker.subscriptions.ShareName;
import io.moquette.broker.subscriptions.SharedSubscription;
import io.moquette.broker.subscriptions.Subscription;
import io.moquette.broker.subscriptions.Topic;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.BasicDataType;
import org.h2.mvstore.type.StringDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class H2SubscriptionsRepository implements ISubscriptionsRepository {

    private static final Logger LOG = LoggerFactory.getLogger(H2SubscriptionsRepository.class);
    private static final String SUBSCRIPTIONS_MAP = "subscriptions";
    private static final String SHARED_SUBSCRIPTIONS_MAP = "shared_subscriptions";
    private final MVStore mvStore;
    private final MVMap.Builder<Couple<ShareName, Topic>, Integer> submapBuilder;

    private MVMap<String, Subscription> subscriptions;
    // clientId -> shared subscription map name
    private MVMap<String, String> sharedSubscriptions;

    H2SubscriptionsRepository(MVStore mvStore) {
        this.mvStore = mvStore;

        submapBuilder = new MVMap.Builder<Couple<ShareName, Topic>, Integer>()
            .keyType(new CoupleValueType());

        this.subscriptions = mvStore.openMap(SUBSCRIPTIONS_MAP);
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
            LOG.info("Removing all shared subscription of a non existing client: {}", clientId);
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
        MVMap<Couple<ShareName, Topic>, Integer> subMap = mvStore.openMap(sharedSubsMapName, submapBuilder);
        Couple<ShareName, Topic> sharedSubKey = Couple.of(share, topicFilter);

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

    @Override
    public void addNewSharedSubscription(String clientId, ShareName share, Topic topicFilter, MqttQoS requestedQoS) {
        String sharedSubsMapName = sharedSubscriptions.computeIfAbsent(clientId,
            H2SubscriptionsRepository::computeShareSubscriptionSubMap);

        // maps the couple (share name, topic) to requested qos
        MVMap<Couple<ShareName, Topic>, Integer> subMap = mvStore.openMap(sharedSubsMapName, submapBuilder);
        subMap.put(Couple.of(share, topicFilter), requestedQoS.value());
    }

    @Override
    public Collection<SharedSubscription> listAllSharedSubscription() {
        List<SharedSubscription> result = new ArrayList<>();

        for (Map.Entry<String, String> entry : sharedSubscriptions.entrySet()) {
            String clientId = entry.getKey();
            String sharedSubsMapName = entry.getValue();

            MVMap<Couple<ShareName, Topic>, Integer> subMap = mvStore.openMap(sharedSubsMapName, submapBuilder);
            for (Map.Entry<Couple<ShareName, Topic>, Integer> subEntry : subMap.entrySet()) {
                final ShareName shareName = subEntry.getKey().v1;
                final Topic topicFilter = subEntry.getKey().v2;
                final MqttQoS qos = MqttQoS.valueOf(subEntry.getValue());
                result.add(new SharedSubscription(shareName, topicFilter, clientId, qos));
            }
        }

        return result;
    }

    private static String computeShareSubscriptionSubMap(String sessionId) {
        return SHARED_SUBSCRIPTIONS_MAP + "_" + sessionId;
    }

    static final class CoupleValueType extends BasicDataType<Couple<ShareName, Topic>> {

        private final Comparator<Couple<ShareName, Topic>> coupleComparator =
            Comparator.<Couple<ShareName, Topic>, String>comparing(c -> c.v1.getShareName())
            .thenComparing(c -> c.v2.toString());

        @Override
        public int compare(Couple<ShareName, Topic> var1, Couple<ShareName, Topic> var2) {
            return coupleComparator.compare(var1, var2);
        }

        @Override
        public int getMemory(Couple<ShareName, Topic> couple) {
            return StringDataType.INSTANCE.getMemory(couple.v1.getShareName()) +
                   StringDataType.INSTANCE.getMemory(couple.v2.toString());
        }

        @Override
        public void write(WriteBuffer buff, Couple<ShareName, Topic> couple) {
            StringDataType.INSTANCE.write(buff, couple.v1.getShareName());
            StringDataType.INSTANCE.write(buff, couple.v2.toString());
        }

        @Override
        public Couple<ShareName, Topic> read(ByteBuffer buffer) {
            String shareName = StringDataType.INSTANCE.read(buffer);
            String topicFilter = StringDataType.INSTANCE.read(buffer);
            return new Couple<>(new ShareName(shareName), Topic.asTopic(topicFilter));
        }

        @Override
        public Couple<ShareName, Topic>[] createStorage(int i) {
            return new Couple[i];
        }
    }
}
