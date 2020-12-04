package com.github.sukhinin.demo;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class DemoSerde implements Serde<byte[]> {

    @Override
    public Serializer<byte[]> serializer() {
        final ConcurrentAccessDetector detector = new ConcurrentAccessDetector();
        return (topic, data) -> {
            detector.throwOnConcurrentAccess();
            return data;
        };
    }

    @Override
    public Deserializer<byte[]> deserializer() {
        final ConcurrentAccessDetector detector = new ConcurrentAccessDetector();
        return (topic, data) -> {
            detector.throwOnConcurrentAccess();
            return data;
        };
    }
}
