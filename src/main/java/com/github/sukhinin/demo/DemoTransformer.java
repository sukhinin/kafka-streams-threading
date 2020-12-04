package com.github.sukhinin.demo;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

public class DemoTransformer implements ValueTransformer<byte[], byte[]> {

    private final ConcurrentAccessDetector detector = new ConcurrentAccessDetector();

    @Override
    public void init(ProcessorContext context) {
        // Do nothing
    }

    @Override
    public byte[] transform(byte[] value) {
        detector.throwOnConcurrentAccess();
        return value;
    }

    @Override
    public void close() {
        // Do nothing
    }
}
