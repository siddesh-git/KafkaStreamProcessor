package com.doodle;

import com.google.gson.Gson;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IbMessageProcessor {

    ExecutorService executorService;
    Map<String, List<String>> cache = new LinkedHashMap<>();//Stored uid's for each minute
    Calendar calendar = Calendar.getInstance();
    Gson g = new Gson();
    SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm"); // Format used to extract uid's at minute level as a key.
    // If needed for hour then format would be yyyy-MM-dd HH. If needed per day then format would be yyyy-MM-dd
    String lastKey;
    MessagePublisher messagePublisher;
    IbMessageProcessor(MessagePublisher messagePublisher){
        this.messagePublisher = messagePublisher;
        executorService = Executors.newFixedThreadPool(1);
    }

    public void readMessage(String msg) throws ParseException {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                InboundMessage inboundMessage = g.fromJson(msg, InboundMessage.class);//Extracting required attributes from json parse
                if(inboundMessage!=null && inboundMessage.getTs()!=null) {
                    calendar.setTimeInMillis(Long.valueOf(inboundMessage.getTs() + "000"));//Since the inbound message has ts in seconds converting to millis for calendar time
                    String date = sf.format(calendar.getTime());//Key in the string format of yyyy-MM-dd HH:mm

                    //Below logic is to extact all uid's for a minute key until new minute key is found.
                    // Once new minute is arrived from incoming message then
                    // the extracted uid's for the previous minute is removed from cache and
                    // sent to messagePublisher to publish the data and new minute uid's will be added to cache.
                    // Process repeats for each new minute and published to messagePublisher
                    if (cache.get(date) != null) {
                        cache.get(date).add(inboundMessage.uid);
                    } else {
                        if (lastKey != null) {
                            List<String> lastValue = cache.get(lastKey);
                            messagePublisher.sendMessage(lastKey, lastValue);
                            cache.remove(lastKey);
                        }
                        List<String> uidList = new ArrayList<>();
                        uidList.add(inboundMessage.getUid());
                        cache.put(date, uidList);
                    }
                    lastKey = date;
                }
            }
        });
    }

    public static void main(String[] args) throws ParseException, IOException {


        Properties properties = new Properties();
        properties.load(new FileInputStream("src/main/resources/kafka.properties"));
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
        kafkaConfiguration.loadConfig(properties);
        KafkaProducerUtil kafkaProducerUtil = new KafkaProducerUtil(kafkaConfiguration);
        MessagePublisher messagePublisher = new MessagePublisher(kafkaProducerUtil);
        IbMessageProcessor ibMessageProcessor = new IbMessageProcessor(messagePublisher);

        //below message can also be passed in std-in
        long time = 1635443098;
        String msg = "{\"uid\":\"abc\", \"ts\":"+time+"}";
        ibMessageProcessor.readMessage(msg);
        time+=600;
        msg = "{\"uid\":\"def\", \"ts\":"+time+"}";
        ibMessageProcessor.readMessage(msg);
        time+=600;
        msg = "{\"uid\":\"gf\", \"ts\":"+time+"}";
        ibMessageProcessor.readMessage(msg);
    }
}
