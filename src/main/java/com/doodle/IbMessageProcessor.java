package com.doodle;

import com.google.gson.Gson;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IbMessageProcessor {

    ExecutorService executorService;
    Map<String, List<String>> cache = new LinkedHashMap<>();
    Calendar calendar = Calendar.getInstance();
    Gson g = new Gson();
    SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
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
                InboundMessage inboundMessage = g.fromJson(msg, InboundMessage.class);
                if(inboundMessage!=null && inboundMessage.getTs()!=null) {
                    calendar.setTimeInMillis(Long.valueOf(inboundMessage.getTs() + "000"));
                    //System.out.println(Long.valueOf(inboundMessage.getTs() + "000"));
                    String date = sf.format(calendar.getTime());
                    //System.out.println(inboundMessage.getUid() + " " + date);
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
}
