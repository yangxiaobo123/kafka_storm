package com.yang.test;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class MyKafkaTopic {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092,node3:9092");
        AdminClient adminClient = AdminClient.create(properties);
        adminClient.createTopics(Arrays.asList(new NewTopic("a1", 3, (short) 1)));
        ListTopicsResult list = adminClient.listTopics();
        Set<String> strings = list.names().get();
        strings.forEach(name -> System.out.println(name));
        DescribeTopicsResult k1 = adminClient.describeTopics(Arrays.asList("k1"));
        Map<String, KafkaFuture<TopicDescription>> values = k1.values();
        values.forEach((k, v) -> System.out.println(k + "\t" + v));
        adminClient.deleteTopics(Arrays.asList("k1"));
        adminClient.close();
    }
}
