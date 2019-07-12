package org.inaetics.pubsub.impl.pubsubadmin.nats;

import io.nats.client.Connection;
import org.inaetics.pubsub.api.Publisher;
import org.inaetics.pubsub.spi.serialization.Serializer;
import org.inaetics.pubsub.spi.utils.Utils;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceFactory;
import org.osgi.framework.ServiceRegistration;

import java.nio.ByteBuffer;
import java.util.*;

public class NatsTopicSender implements ServiceFactory<Publisher> {

    private final Connection connection;
    private final BundleContext bundleContext;

    private final Map<Long, Publisher> publishers = new HashMap<>();
    private final UUID uuid;
    private ServiceRegistration<Publisher> registration;
    private final String topic;
    private final Serializer serializer;

    public NatsTopicSender(Connection connection, BundleContext bundleContext, String topic, Serializer serializer) {
        this.uuid = UUID.randomUUID();
        this.connection = connection;
        this.bundleContext = bundleContext;
        this.topic = topic;
        this.serializer = serializer;
    }

    @Override
    public Publisher getService(Bundle bundle, ServiceRegistration<Publisher> serviceRegistration) {
        synchronized (this.publishers) {
            if (!publishers.containsKey(bundle.getBundleId())) {
                //new
                Publisher pub = new Publisher() {
                    @Override
                    public void send(Object msg) {
                        sendMsg(msg);
                    }
                };
                publishers.put(bundle.getBundleId(), pub);
            }
            return publishers.get(bundle.getBundleId());
        }
    }

    public void start() {
        Dictionary<String, Object> properties = new Hashtable<>();
        properties.put(Publisher.PUBSUB_TOPIC, topic);

        registration = bundleContext.registerService(Publisher.class, this, properties);
    }

    public void stop() {
        registration.unregister();
    }

    private void sendMsg(Object msg) {
        // Serialize message
        final byte[] message = serializer.serialize(msg);

        // Create payload array (header + message size)
        byte[] payload = new byte[NatsConstants.HEADER_SIZE + message.length]; //first 4 hashcode of the class
        ByteBuffer buff = ByteBuffer.wrap(payload);
        int typeId = Utils.typeIdForClass(msg.getClass());
        buff.putInt(typeId);
        buff.put(4, (byte)0); //TODO major version
        buff.put(5, (byte)0); //TODO minor version
        buff.put(message);

        connection.publish(topic, payload);
    }

    public UUID getUuid() {
        return uuid;
    }

    @Override
    public void ungetService(Bundle bundle, ServiceRegistration<Publisher> serviceRegistration, Publisher publisher) {
        synchronized (this.publishers) {
            publishers.remove(bundle.getBundleId());
        }
    }
}
