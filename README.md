Kafka Streams demo for https://stackoverflow.com/q/65136632:
1. Serializers and deserializers are accessed concurrently from multiple threads and thus must be thread-safe.
2. Transformers seem to be invoked from only a single thread at a time.