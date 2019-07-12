package org.inaetics.pubsub.impl.pubsubadmin.nats;

import io.nats.client.Connection;
import io.nats.client.Nats;
import org.inaetics.pubsub.spi.pubsubadmin.PubSubAdmin;
import org.inaetics.pubsub.spi.serialization.Serializer;
import org.inaetics.pubsub.spi.utils.Constants;
import org.inaetics.pubsub.spi.utils.Utils;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;
import org.osgi.service.log.LogService;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import static org.osgi.framework.Constants.SERVICE_ID;

public class NatsPubSubAdmin implements PubSubAdmin {
    public static final String SERVICE_PID = NatsPubSubAdmin.class.getName();

    private final Map<Long, ServiceReference<Serializer>> serializers = new Hashtable<>();

    private final Map<String, NatsTopicSender> senders = new Hashtable<>();
    private final Map<String, NatsTopicReceiver> receivers = new Hashtable<>();

    private final double sampleScore = NatsConstants.NATS_DEFAULT_SAMPLE_SCORE; /*TODO make configureable*/
    private final double controlScore = NatsConstants.NATS_DEFAULT_CONTROL_SCORE; /*TODO make configureable*/
    private final double noQosScore = NatsConstants.NATS_DEFAULT_NO_QOS_SCORE;

    private final BundleContext bundleContext = FrameworkUtil.getBundle(NatsPubSubAdmin.class).getBundleContext();

    private volatile LogService log;

    private Connection connection;

    public void start() throws IOException, InterruptedException {
        log.log(LogService.LOG_INFO, "Initiating connection...");
        connection = Nats.connect("nats://nats-main:4222");
        log.log(LogService.LOG_INFO, "Connection initiated.");
    }

    public void stop() throws InterruptedException {
        log.log(LogService.LOG_INFO, "Closing connection...");
        connection.close();
        log.log(LogService.LOG_INFO, "Connection closed.");
    }

    public synchronized void addSerializer(ServiceReference<Serializer> serRef) {
        long svcId = (Long) serRef.getProperty(SERVICE_ID);
        this.serializers.put(svcId, serRef);
    }

    public synchronized void removeSerializer(ServiceReference<Serializer> serRef) {
        long svcId = (Long) serRef.getProperty(SERVICE_ID);
        this.serializers.remove(svcId);
    }

    @Override
    public MatchResult matchPublisher(long publisherBndId, final String svcFilter) {
        return Utils.matchPublisher(
                bundleContext,
                publisherBndId,
                svcFilter,
                NatsConstants.NATS_ADMIN_TYPE,
                sampleScore,
                controlScore,
                noQosScore);
    }

    @Override
    public MatchResult matchSubscriber(long svcProviderBndId, final Properties svcProperties) {
        return Utils.matchSubscriber(
                bundleContext,
                svcProviderBndId,
                svcProperties,
                NatsConstants.NATS_ADMIN_TYPE,
                sampleScore,
                controlScore,
                noQosScore);
    }


    @Override
    public boolean matchDiscoveredEndpoint(final Properties endpoint) {
        return NatsConstants.NATS_ADMIN_TYPE.equals(endpoint.get(Constants.PUBSUB_ENDPOINT_ADMIN_TYPE));
    }

    @Override
    public Properties setupTopicSender(String scope, String topic, Properties topicProperties, long serializerSvcId) {
        Properties endpoint = null;
        ServiceReference<Serializer> serRef = this.serializers.get(serializerSvcId);
        Serializer ser = null;
        if (serRef != null) {
            ser = bundleContext.getService(serRef);
        }
        if (ser != null) {
            NatsTopicSender sender = new NatsTopicSender(connection, bundleContext, topic, ser);
            String key = scope + "::" + topic;
            senders.put(key, sender);
            sender.start();

            endpoint = new Properties();
            endpoint.put(Constants.PUBSUB_ENDPOINT_ADMIN_TYPE, NatsConstants.NATS_ADMIN_TYPE);
            endpoint.put(Constants.PUBSUB_ENDPOINT_TYPE, Constants.PUBSUB_PUBLISHER_ENDPOINT_TYPE);
            endpoint.put(Constants.PUBSUB_ENDPOINT_UUID, sender.getUuid());
            endpoint.put(Constants.PUBSUB_ENDPOINT_TOPIC_NAME, topic);
            endpoint.put(Constants.PUBSUB_ENDPOINT_TOPIC_SCOPE, scope);
            endpoint.put(Constants.PUBSUB_ENDPOINT_SERIALIZER, serRef.getProperty(Serializer.SERIALIZER_NAME_KEY));
            endpoint.put(Constants.PUBSUB_ENDPOINT_VISBILITY, Constants.PUBSUB_SUBSCRIBER_SYSTEM_VISIBLITY);
            endpoint.put(Constants.PUBSUB_ENDPOINT_FRAMEWORK_UUID, Utils.getFrameworkUUID(bundleContext));
        }

        return endpoint; //NOTE can be null
    }

    @Override
    public void teardownTopicSender(String scope, String topic) {
        String key = scope + "::" + topic;
        NatsTopicSender sender = senders.get(key);
        if (senders != null) {
            sender.stop();
        } else {
            log.log(LogService.LOG_WARNING, String.format("Cannot teardown topic sender for %s/%s. Does not exist!", scope, topic));
        }
    }

    @Override
    public Properties setupTopicReceiver(String scope, String topic, Properties topicProperties, long serializerSvcId) {
        Properties endpoint = null;
        ServiceReference<Serializer> serRef = this.serializers.get(serializerSvcId);
        Serializer ser = null;
        if (serRef != null) {
            ser = bundleContext.getService(serRef);
        }
        if (ser != null) {
            NatsTopicReceiver receiver = new NatsTopicReceiver(bundleContext, connection, scope, topic, ser);
            String key = scope + "::" + topic;
            receivers.put(key, receiver);
            receiver.start();

            endpoint = new Properties();
            endpoint.put(Constants.PUBSUB_ENDPOINT_ADMIN_TYPE, NatsConstants.NATS_ADMIN_TYPE);
            endpoint.put(Constants.PUBSUB_ENDPOINT_TYPE, Constants.PUBSUB_SUBSCRIBER_ENDPOINT_TYPE);
            endpoint.put(Constants.PUBSUB_ENDPOINT_UUID, receiver.getUuid());
            endpoint.put(Constants.PUBSUB_ENDPOINT_TOPIC_NAME, topic);
            endpoint.put(Constants.PUBSUB_ENDPOINT_TOPIC_SCOPE, scope);
            endpoint.put(Constants.PUBSUB_ENDPOINT_SERIALIZER, serRef.getProperty(Serializer.SERIALIZER_NAME_KEY));
            endpoint.put(Constants.PUBSUB_ENDPOINT_VISBILITY, Constants.PUBSUB_SUBSCRIBER_SYSTEM_VISIBLITY);
            endpoint.put(Constants.PUBSUB_ENDPOINT_FRAMEWORK_UUID, Utils.getFrameworkUUID(bundleContext));
        }

        return endpoint; //NOTE can be null
    }

    @Override
    public void teardownTopicReceiver(String scope, String topic) {
        String key = scope + "::" + topic;
        NatsTopicReceiver receiver = receivers.get(key);
        if (receiver != null) {
            receiver.stop();
        } else {
            log.log(LogService.LOG_WARNING, String.format("Cannot teardown topic receiver for %s/%s. Does not exist!", scope, topic));
        }
    }

    @Override
    public void addDiscoveredEndpoint(Properties endpoint) {
        log.log(LogService.LOG_INFO, "Adding discovered endpoint " + endpoint);
    }

    @Override
    public void removeDiscoveredEndpoint(Properties endpoint) {
        log.log(LogService.LOG_INFO, "Removing discovery endpoint " + endpoint);
    }
}
