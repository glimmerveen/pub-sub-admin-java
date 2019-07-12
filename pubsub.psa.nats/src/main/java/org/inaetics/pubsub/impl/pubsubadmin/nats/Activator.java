package org.inaetics.pubsub.impl.pubsubadmin.nats;

import org.apache.felix.dm.DependencyActivatorBase;
import org.apache.felix.dm.DependencyManager;
import org.inaetics.pubsub.spi.pubsubadmin.PubSubAdmin;
import org.inaetics.pubsub.spi.serialization.Serializer;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.service.log.LogService;

import java.util.Properties;

public class Activator  extends DependencyActivatorBase {

    @Override
    public void init(BundleContext bundleContext, DependencyManager manager) {
        String[] services = new String[]{PubSubAdmin.class.getName()};


        Properties properties = new Properties();
        properties.put(Constants.SERVICE_PID, NatsPubSubAdmin.SERVICE_PID);
        properties.put("osgi.command.scope", "pubsub");
        properties.put("osgi.command.function", new String[]{"nats"});

        manager.add(
                manager.createComponent()
                        .setInterface(services, properties)
                        .setImplementation(NatsPubSubAdmin.class)
                        .setCallbacks(null, "start", "stop", null)
                        .add(createServiceDependency()
                                .setService(LogService.class)
                                .setRequired(true))
                        .add(createServiceDependency()
                                .setService(Serializer.class)
                                .setCallbacks("addSerializer", "removeSerializer"))
        );

    }
}
