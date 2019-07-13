package org.inaetics.pubsub.examples.pubsub.subscriber;

import org.inaetics.pubsub.api.Subscriber;
import org.inaetics.pubsub.examples.pubsub.common.Location;
import org.inaetics.pubsub.examples.pubsub.common.PointOfInterrest;
import org.osgi.service.component.annotations.Component;

import java.util.concurrent.LinkedBlockingQueue;

@Component(
        immediate = true,
        service = Subscriber.class,
        property = Subscriber.PUBSUB_TOPIC + "=poi1"
)
public class DemoSubscriber implements Subscriber<Location> {

    @Override
    public Class<Location> receiveClass() {
        return Location.class;
    }

    @Override
    public void init() {
        //nop
    }

    @Override
    public void receive(Location location) {
        System.out.printf("Recv location [%s, %s]\n", location.getLat(), location.getLon());
    }

    public void receive(PointOfInterrest poi) {
        System.out.printf("Recv poi [%s, %s, %s]\n", poi.getName(), poi.getLocation().getLat(), poi.getLocation().getLon());
    }
}