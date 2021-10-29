package com.doodle;

import com.google.gson.JsonObject;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MessagePublisher {
    ExecutorService executorService;
    KafkaProducerUtil kafkaProducerUtil;
    MessagePublisher(KafkaProducerUtil kafkaProducerUtil){
        executorService = Executors.newFixedThreadPool(10); //Pool of 10 threads that write processed results to target kafka topic
        this.kafkaProducerUtil = kafkaProducerUtil;
    }
    public void sendMessage(String time, List<String> uids){
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                Set<String> uniqueUids = new HashSet<String>(uids);// Finding unique uid's from the duplicate list in given timeframe
                JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty("time", time);
                jsonObject.addProperty("uidCount", uniqueUids.size());
                kafkaProducerUtil.produce(jsonObject.toString());
            }
        });
    }
}
