package com.github.fredoliveira.userenricher;

import com.github.fredoliveira.bankbalance.BankBalanceStreams;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class UserEnricher {

    private static Logger logger = LoggerFactory.getLogger(BankBalanceStreams.class);

    public static void main(String[] args) {
        var props = getProperties();

        StreamsBuilder builder = new StreamsBuilder();
        GlobalKTable<String, String> usersGlobalTable = builder.globalTable("user-table");
        KStream<String, String> userPurchases = builder.stream("user-purchases");

        buildTopologyUsingJoin(usersGlobalTable, userPurchases);
        buildTopologyUsingLeftJoin(usersGlobalTable, userPurchases);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static void buildTopologyUsingJoin(GlobalKTable<String, String> usersGlobalTable, KStream<String, String> userPurchases) {
        userPurchases
                .join(
                        usersGlobalTable,
                        (key, value) -> key,
                        (userPurchase, userInfo) -> "Purchase=" + userPurchase + ",UserInfo=[" + userInfo + "]")
                .peek((k,v) -> logger.info("Observed event: {}", v))
                .to("user-purchases-enriched-inner-join", Produced.with(Serdes.String(), Serdes.String()));
    }

    static void buildTopologyUsingLeftJoin(GlobalKTable<String, String> usersGlobalTable, KStream<String, String> userPurchases) {
        userPurchases
                .leftJoin(
                        usersGlobalTable,
                        (key, value) -> key,
                        (userPurchase, userInfo) -> {
                            if (userInfo != null) {
                                return "Purchase=" + userPurchase + ",UserInfo=[" + userInfo + "]";
                            } else {
                                return "Purchase=" + userPurchase + ",UserInfo=null";
                            }
                        })
                .peek((k,v) -> logger.info("Observed event: {}", v))
                .to("user-purchases-enriched-left-join", Produced.with(Serdes.String(), Serdes.String()));
    }

    private static Properties getProperties() {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-enricher");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        return props;
    }
}
