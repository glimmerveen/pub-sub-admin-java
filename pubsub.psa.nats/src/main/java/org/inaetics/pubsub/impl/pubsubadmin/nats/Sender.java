package org.inaetics.pubsub.impl.pubsubadmin.nats;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class Sender {
    public static void main(String ... args) throws Exception {
        final Options options = new Options.Builder().server("nats://localhost:4222").build();
        try (final Connection connect = Nats.connect(options)) {
            int counter = 0;
            while (true) {
                System.out.println("Sending " + counter);
                connect.publish("test", ("Hello World: " + counter++).getBytes(StandardCharsets.UTF_8));
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            }
        }
    }
}
