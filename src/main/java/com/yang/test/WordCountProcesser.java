package com.yang.test;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;

import java.time.Duration;
import java.util.HashMap;

public class WordCountProcesser implements Processor<String, String> {
    private HashMap<String, Long> map;
    private ProcessorContext processorContext;

    @Override
    public void init(ProcessorContext processorContext) {
        map = new HashMap<>();
        this.processorContext = processorContext;
        this.processorContext.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timeStamp -> {
            System.out.println("=========" + timeStamp + "==========");
            map.forEach((k, v) -> {
                System.out.println(k + "\t" + v);
                this.processorContext.forward(k, v);
            });

        });
    }

    @Override
    public void process(String key, String value) {
        String[] words = value.split(" ");
        for (String word : words) {
            Long num = map.getOrDefault(word, 0L);
            num++;
            map.put(word, num);
        }
        processorContext.commit();
    }

    @Override
    public void close() {

    }
}
