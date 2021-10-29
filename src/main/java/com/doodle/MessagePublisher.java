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
        executorService = Executors.newFixedThreadPool(10);
        this.kafkaProducerUtil = kafkaProducerUtil;
    }
    public void sendMessage(String time, List<String> uids){
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                System.out.println(uids.size());
                Set<String> uniqueUids = new HashSet<String>(uids);
                JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty("time", time);
                jsonObject.addProperty("uidCount", uniqueUids.size());
                kafkaProducerUtil.produce(jsonObject.toString());
            }
        });
    }
}
