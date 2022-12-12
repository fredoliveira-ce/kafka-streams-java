package com.github.fredoliveira.bankbalance;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionsProducer {

    public static void main(String[] args) {
        var properties = getProperties();
        Producer<String, String> producer = new KafkaProducer<>(properties);

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
            } catch (InterruptedException e) {
                break;
            }
        }
        producer.close();
    }

    public static ProducerRecord<String, String> newRandomTransaction(String name) {
        var transaction = JsonNodeFactory.instance.objectNode();
        var amount = ThreadLocalRandom.current().nextInt(0, 100);

        transaction.put("name", name);
        transaction.put("amount", amount);
        transaction.put("time", Instant.now().toString());

        return new ProducerRecord<>("bank-transactions", name, transaction.toString());
    }

    private static Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return properties;
    }

}
