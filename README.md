# kafka-streams-java

## Word Count
Create topics
```
kafka-topics --bootstrap-server localhost:9092 --create --topic word-count-input --partitions 2 --replication-factor 1
```
```
kafka-topics --bootstrap-server localhost:9092 --create --topic word-count-output --partitions 2 --replication-factor 1
```

Check it out
```
kafka-topics --bootstrap-server localhost:9092 --list
```

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

## Bank Balance
Create topics
```
kafka-topics --bootstrap-server localhost:9092 --create --topic bank-transactions --partitions 2 --replication-factor 1
```
```
kafka-topics --bootstrap-server localhost:9092 --create --topic bank-balance-aggregated --partitions 2 --replication-factor 1
```

Kafka consumer
```
kafka-console-consumer --bootstrap-server localhost:9092 \
        --topic bank-balance-aggregated \
        --formatter kafka.tools.DefaultMessageFormatter \
        --property print.timestamp=true \
        --property print.key=true \
        --property print.value=true \
        --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
        --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer \
        --from-beginning
```

## User Enricher
Create topics
```
kafka-topics --bootstrap-server localhost:9092 --create --topic user-table --partitions 2 --replication-factor 1
```
```
kafka-topics --bootstrap-server localhost:9092 --create --topic user-purchases --partitions 2 --replication-factor 1
```
```
kafka-topics --bootstrap-server localhost:9092 --create --topic user-purchases-enriched-inner-join --partitions 2 --replication-factor 1
```
```
kafka-topics --bootstrap-server localhost:9092 --create --topic user-purchases-enriched-left-join --partitions 2 --replication-factor 1
```
Kafka consumer
```
kafka-console-consumer --bootstrap-server localhost:9092 \
        --topic user-purchases-enriched-inner-join \
        --formatter kafka.tools.DefaultMessageFormatter \
        --property print.timestamp=true \
        --property print.key=true \
        --property print.value=true \
        --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
        --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
        --from-beginning
```
```
kafka-console-consumer --bootstrap-server localhost:9092 \
        --topic user-purchases-enriched-left-join \
        --formatter kafka.tools.DefaultMessageFormatter \
        --property print.timestamp=true \
        --property print.key=true \
        --property print.value=true \
        --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
        --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
        --from-beginning
```


