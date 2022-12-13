package com.github.fredoliveira.wordcount;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

class WordCountTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    @BeforeEach
    void setup() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        WordCount app = new WordCount();
        Topology topology = app.createTopology();
        testDriver = new TopologyTestDriver(topology, config);
        inputTopic = testDriver.createInputTopic("word-count-input", new StringSerializer(), new StringSerializer());
        outputTopic = testDriver.createOutputTopic("word-count-output", new StringDeserializer(), new LongDeserializer());
    }

    @AfterEach
    void close() {
        testDriver.close();
    }

    @Test
    public void test() {
        Assertions.assertEquals("1", "1");
    }

    @Test
    public void makeSureCountsAreCorrect() {
        inputTopic.pipeInput("hello hello hello other");

        Assertions.assertEquals(
                Map.of("other", "1", "hello","3").toString(),
                outputTopic.readKeyValuesToMap().toString()
        );
        Assertions.assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void testCountListOfWords() {
        final List<String> inputValues = Arrays.asList(
                "Apache Kafka Streams Example",
                "Using Kafka Streams Test Utils",
                "Reading and Writing Kafka Topic"
        );
        final Map<String, Long> expectedWordCounts = new HashMap<>();
        expectedWordCounts.put("apache", 1L);
        expectedWordCounts.put("kafka", 3L);
        expectedWordCounts.put("streams", 2L);
        expectedWordCounts.put("example", 1L);
        expectedWordCounts.put("using", 1L);
        expectedWordCounts.put("test", 1L);
        expectedWordCounts.put("utils", 1L);
        expectedWordCounts.put("reading", 1L);
        expectedWordCounts.put("and", 1L);
        expectedWordCounts.put("writing", 1L);
        expectedWordCounts.put("topic", 1L);

        inputTopic.pipeValueList(inputValues);
        final Map<String, Long> actualWordCounts = outputTopic.readKeyValuesToMap();
        Assertions.assertEquals(actualWordCounts, expectedWordCounts);
    }
}