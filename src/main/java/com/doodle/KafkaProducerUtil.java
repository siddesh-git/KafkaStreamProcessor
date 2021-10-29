package com.doodle;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class KafkaProducerUtil {
    Producer<String, String> producer;
    KafkaConfiguration kafkaConfiguration;
    KafkaProducerUtil(KafkaConfiguration kafkaConfiguration){
        this.kafkaConfiguration = kafkaConfiguration;
        producer = createProducer();
    }
    private Producer<String, String> createProducer() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaConfiguration.getProducerBootstrapServerUrl());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    public void produce(String message){

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(kafkaConfiguration.getProducerTopic(), message);
        producer.send(record);
        producer.flush();
        System.out.println(message+" has been published");
        //System.out.println("Completed");
    }

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream("../../../resources/kafka.properties"));
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
        KafkaProducerUtil kafkaProducerUtil = new KafkaProducerUtil(kafkaConfiguration);
        long time = 1635443098;
        int i=0;
        String m1;
        while(i<10){
            i++;
            m1 = "{\"uid\":\"abc\", \"ts\":"+time+"}";
            kafkaProducerUtil.produce(m1);
            m1 = "{\"uid\":\"ret\", \"ts\":"+time+"}";
            kafkaProducerUtil.produce(m1);
            time+=600;
            m1 = "{\"uid\":\"def\", \"ts\":"+time+"}";
            kafkaProducerUtil.produce(m1);
            m1 = "{\"uid\":\"gf\", \"ts\":"+time+"}";
            kafkaProducerUtil.produce(m1);
        }
        kafkaProducerUtil.producer.close();
    }
}
