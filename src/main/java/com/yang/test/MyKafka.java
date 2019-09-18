package com.yang.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class MyKafka {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092,node3:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 2048);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1000);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 5000);
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(properties);
        producer.initTransactions();
        try {
            producer.beginTransaction();
            ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>("k1", 1, "hh");
            producer.send(record);
            producer.commitTransaction();
        } catch (Exception e) {
            e.printStackTrace();
            producer.abortTransaction();
        } finally {
            producer.flush();
            producer.close();
        }


    }
}
