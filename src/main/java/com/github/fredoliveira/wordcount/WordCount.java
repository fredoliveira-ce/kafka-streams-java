package com.github.fredoliveira.wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Properties;

public class WordCount {

    public static void main(String[] args) {
        KafkaStreams streams = new KafkaStreams(createTopology(), getProperties());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    @NotNull
    static Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("word-count-input");
        KTable<String, Long> wordCount = stream
                .mapValues(text -> text.toLowerCase())
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                .selectKey((key, word) -> word)
                .groupByKey()
                .count(Named.as("counts"));

        wordCount.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    @NotNull
    private static Properties getProperties() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return config;
    }


}
