package com.yang.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class MyKafkaConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092,node3:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(properties);
        consumer.subscribe(Arrays.asList("k1"));
//        consumer.assign(Arrays.asList(new TopicPartition("k1",0)));
//        consumer.seek(new TopicPartition("k1",0),0);
        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(5));
            records.forEach(record -> {
                System.out.println(record.key() + record.value() + record.offset());
                consumer.commitAsync();
            });
        }
    }
}
