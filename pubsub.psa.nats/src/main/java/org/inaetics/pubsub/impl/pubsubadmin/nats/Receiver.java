package org.inaetics.pubsub.impl.pubsubadmin.nats;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.nio.charset.StandardCharsets;

public class Receiver {
    public static void main(String ... args) throws Exception {
        final Options options = new Options.Builder().server("nats://localhost:4222").build();
        try (final Connection connect = Nats.connect(options)) {
            connect.createDispatcher(
                    (msg) -> System.out.println(new String(msg.getData(), StandardCharsets.UTF_8))
            ).subscribe("test");

            System.out.println("Press any key to stop...");
            System.in.read();
            options.getExecutor().shutdown();
        }
    }
}
