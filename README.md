# kafka-streams-java
```kafka-topics --bootstrap-server localhost:9092 --create --topic word-count-input --partitions 2 --replication-factor 1```
```kafka-topics --bootstrap-server localhost:9092 --create --topic word-count-output --partitions 2 --replication-factor 1```

Check it out
```kafka-topics --bootstrap-server localhost:9092 --list```

Kafka consumer
```
kafka-console-consumer --bootstrap-server localhost:9092 \
        --topic word-count-output \
        --formatter kafka.tools.DefaultMessageFormatter \
        --property print.timestamp=true \
        --property print.key=true \
        --property print.value=true \
        --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
        --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer \
        --from-beginning
```

Kafka producer
```
kafka-console-producer --bootstrap-server localhost:9092 --topic word-count-input
```
