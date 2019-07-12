package org.inaetics.pubsub.impl.pubsubadmin.nats;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import org.inaetics.pubsub.api.Subscriber;
import org.inaetics.pubsub.spi.serialization.Serializer;
import org.inaetics.pubsub.spi.utils.Utils;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Filter;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

import java.nio.ByteBuffer;
import java.util.*;

import static org.osgi.framework.Constants.OBJECTCLASS;

public class NatsTopicReceiver implements MessageHandler {
    private final BundleContext bundleContext;
    private final Connection connection;
    private final String topic;
    private final String scope;
    private final ServiceTracker<Subscriber, Subscriber> subscriberTrackers;
    private final Map<Long, SubscriberEntry> subscribers = new HashMap<>();
    private final Map<Integer, Class<?>> typeIdMap = new HashMap<>();
    private final Serializer serializer;
    private final UUID uuid;
    private Dispatcher dispatcher;

    private class SubscriberEntry {
        private final long svcId;
        private final Subscriber subscriber;
        private final Collection<Class<?>> receiveClasses;
        boolean initialized = false;

        public SubscriberEntry(long svcId, Subscriber subscriber, Class<?> receiveClass) {
            this.svcId = svcId;
            this.subscriber = subscriber;
            this.receiveClasses = Collections.unmodifiableCollection(Arrays.asList(receiveClass));
        }
    }

    public NatsTopicReceiver(BundleContext bundleContext, Connection connection, String topic, String scope, Serializer serializer) {
        this.bundleContext = bundleContext;
        this.connection = connection;
        this.topic = topic;
        this.scope = scope;
        this.serializer = serializer;
        this.uuid = UUID.randomUUID();

        String filter = String.format("(&(%s=%s)(%s=%s))", OBJECTCLASS, Subscriber.class.getName(), org.inaetics.pubsub.api.Constants.TOPIC_KEY, topic);
        Filter f = null;
        try {
            f = bundleContext.createFilter(filter);
        } catch (InvalidSyntaxException e) {
            e.printStackTrace();
        }

        this.subscriberTrackers = new ServiceTracker<Subscriber, Subscriber>(bundleContext, f, new ServiceTrackerCustomizer<Subscriber, Subscriber>() {
            @Override
            public Subscriber addingService(ServiceReference<Subscriber> serviceReference) {
                Subscriber subscriber = bundleContext.getService(serviceReference);
                addSubscriber(serviceReference);
                return subscriber;
            }

            @Override
            public void modifiedService(ServiceReference<Subscriber> serviceReference, Subscriber subscriber) {

            }

            @Override
            public void removedService(ServiceReference<Subscriber> serviceReference, Subscriber subscriber) {
                removeSubscriber(serviceReference);
            }
        });
    }

    private void addSubscriber(ServiceReference<Subscriber> subscriber) {
        boolean match = isMatch(subscriber);
        if (match) {
            synchronized (subscribers) {
                Long svcId = (Long) subscriber.getProperty("service.id");
                Subscriber<?> sub = bundleContext.getService(subscriber);
                SubscriberEntry entry = new SubscriberEntry(svcId, sub, sub.receiveClass());
                subscribers.put(svcId, entry);
            }
            synchronized (typeIdMap) {
                Subscriber<?> sub = bundleContext.getService(subscriber);
                int typeId = Utils.typeIdForClass(sub.receiveClass());
                typeIdMap.put(typeId, sub.receiveClass());
            }
        }
    }

    boolean isMatch(ServiceReference<?> ref) {
        String subScope = (String)ref.getProperty(Subscriber.PUBSUB_SCOPE);
        boolean match = this.scope.equals(subScope);
        if (subScope == null && this.scope.equals("default")) {
            match = true; //for default scope a subscriber can leave out the scope property
        }
        return match;
    }

    private void removeSubscriber(ServiceReference<Subscriber> subscriber) {
        synchronized (subscribers) {
            subscribers.remove((Long) subscriber.getProperty("service.id"));
        }
    }

    public void start() {
        subscriberTrackers.open();

        dispatcher = connection.createDispatcher(this).subscribe(topic);
    }

    public void stop() {
        connection.closeDispatcher(dispatcher);

        subscriberTrackers.close();
    }

    @Override
    public void onMessage(Message msg) throws InterruptedException {
        ByteBuffer payload = ByteBuffer.wrap(msg.getData());

        final int type = payload.getInt();
        final byte major = payload.get();
        final byte versionMinor = payload.get();

        final byte[] body = payload.array();

        final Class<?> msgClass = typeIdMap.get(type);
        if (msgClass == null) {
            System.err.println("Unable to find id for type " + type);
        } else {
            final Object deserialize = serializer.deserialize(msgClass.getName(), body);
            synchronized (subscribers) {
                for (SubscriberEntry entry : subscribers.values()) {
                    if (!entry.initialized) {
                        if (entry.subscriber != null) {
                            entry.subscriber.init();
                        }
                        entry.initialized = true;
                    }
                    for (Class<?> clazz : entry.receiveClasses) {
                        if (msgClass.equals(clazz)) {
                            if (entry.subscriber != null) {
                                entry.subscriber.receive(deserialize);
                            }
                        }
                    }
                }
            }
        }

    }

    public void nats() {
        System.out.println("Connection: " + connection);
    }

    public UUID getUuid() {
        return uuid;
    }
}
