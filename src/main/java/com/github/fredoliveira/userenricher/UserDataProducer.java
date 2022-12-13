package com.github.fredoliveira.userenricher;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class UserDataProducer {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        var props = getProperties();
        Producer<String, String> producer = new KafkaProducer<>(props);

        System.out.println("\nExample 1 - create a new user, then we send some data to Kafka\n");
        producer.send(userRecord("Joseph", "First=Joseph,Last=Doe,Email=joseph.doe@gmail.com")).get();
        producer.send(purchaseRecord("Joseph", "Apples and Bananas (1)")).get();
        Thread.sleep(10000);


        System.out.println("\nExample 2 - receive user purchase, but it doesn't exist in Kafka\n");
        producer.send(purchaseRecord("bob", "Kafka Udemy Course (2)")).get();
        Thread.sleep(10000);

        System.out.println("\nExample 3 - update user \"john\", and send a new transaction\n");
        producer.send(userRecord("Joseph", "First=Joseph,Last=Junior,Email=joseph.junior@gmail.com")).get();
        producer.send(purchaseRecord("Joseph", "Oranges (3)")).get();
        Thread.sleep(10000);

        System.out.println("\nExample 4 - send a user purchase for stephane, but it exists in Kafka later\n");
        producer.send(purchaseRecord("Kristian", "Computer (4)")).get();
        producer.send(userRecord("Kristian", "First=Kristian,Last=Fantin,GitHub=kristian")).get();
        producer.send(purchaseRecord("Kristian", "Books (4)")).get();
        producer.send(userRecord("Kristian", null)).get();
        Thread.sleep(10000);

        System.out.println("\nExample 5 - create a user, but it gets deleted before any purchase comes through\n");
        producer.send(userRecord("Lucas", "First=Alice")).get();
        producer.send(userRecord("Lucas", null)).get(); // that's the delete record
        producer.send(purchaseRecord("Lucas", "Apache Kafka Series (5)")).get();
        Thread.sleep(10000);

        System.out.println("End");
        producer.close();
    }

    private static ProducerRecord<String, String> userRecord(String key, String value){
        return new ProducerRecord<>("user-table", key, value);
    }

    private static ProducerRecord<String, String> purchaseRecord(String key, String value){
        return new ProducerRecord<>("user-purchases", key, value);
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
