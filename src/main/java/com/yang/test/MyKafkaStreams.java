package com.yang.test;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class MyKafkaStreams {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092,node3:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "word—count");

        Topology topology = new Topology();
        topology.addSource("s1", "k1");
        topology.addProcessor("s2", () -> new WordCountProcesser(), "s1");
        topology.addSink("s3", "k2", new StringSerializer(), new StringSerializer(), "s2");

        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.start();
    }
}
