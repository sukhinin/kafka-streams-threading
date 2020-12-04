package com.github.sukhinin.demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;

public class KafkaStreamsThreadingDemo {

    private static final String DEFAULT_APPLICATION_ID = "kafka-streams-threading-demo";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_TOPIC = "test-topic";

    public static void main(String[] args) {
        final String applicationId = System.getenv().getOrDefault("APPLICATION_ID", DEFAULT_APPLICATION_ID);
        final String bootstrapServers = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS);
        final String topic = System.getenv().getOrDefault("TOPIC", DEFAULT_TOPIC);

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "4");

        final StreamsBuilder builder = new StreamsBuilder();

        // Uncomment one of the following two lines to detect concurrent
        // invocations of either serdes or transformers.
        detectConcurrentSerdeInvocations(builder, topic);
//        detectConcurrentTransformerInvocations(builder, topic);

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    /**
     * Demonstrates Serializers and Deserializers being invoked concurrently from multiple threads.
     * ConcurrentAccessDetector detects such patterns and throws IllegalStateException.
     */
    private static void detectConcurrentSerdeInvocations(final StreamsBuilder builder, final String topic) {
        builder.stream(topic, Consumed.with(new DemoSerde(), new DemoSerde()))
                .print(Printed.toSysOut());
    }

    /**
     * Demonstrates ValueTransformer being invoked from a single thread at a time. No concurrent access
     * has been detected by ConcurrentAccessDetector.
     */
    private static void detectConcurrentTransformerInvocations(final StreamsBuilder builder, final String topic) {
        builder.stream(topic, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                .transformValues(DemoTransformer::new)
                .print(Printed.toSysOut());
    }
}
