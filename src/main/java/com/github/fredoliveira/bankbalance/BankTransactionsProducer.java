package com.github.fredoliveira.bankbalance;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionsProducer {

    static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        var props = getProperties();
        Producer<String, String> producer = new KafkaProducer<>(props);

        int i = 0;
        while (true) {
            System.out.println("Producing batch: " + i);
            try {
                producer.send(newRandomTransaction("Joseph"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("Lucas"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("Kristian"));
                Thread.sleep(100);
                i += 1;
            } catch (InterruptedException | JsonProcessingException e) {
                break;
            }
        }
        producer.close();
    }

    public static ProducerRecord<String, String> newRandomTransaction(String name) throws JsonProcessingException {
        var amount = ThreadLocalRandom.current().nextInt(0, 100);
        var userBalance = new UserBalance(name, amount, Instant.now().toString());
        return new ProducerRecord<>("bank-transactions", name, objectMapper.writeValueAsString(userBalance));
    }

    private static Properties getProperties() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        return props;
    }

}
