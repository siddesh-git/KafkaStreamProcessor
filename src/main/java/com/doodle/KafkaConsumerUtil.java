package com.doodle;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerUtil {

     IbMessageProcessor ibMessageProcessor;
     KafkaConfiguration kafkaConfiguration;
    public KafkaConsumerUtil(IbMessageProcessor ibMessageProcessor, KafkaConfiguration kafkaConfiguration) {
        this.ibMessageProcessor = ibMessageProcessor;
        this.kafkaConfiguration = kafkaConfiguration;
    }

    //Start point of the project.
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream("src/main/resources/kafka.properties"));
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
        kafkaConfiguration.loadConfig(properties);
        KafkaProducerUtil kafkaProducerUtil = new KafkaProducerUtil(kafkaConfiguration);
        MessagePublisher messagePublisher = new MessagePublisher(kafkaProducerUtil);
        IbMessageProcessor ibMessageProcessor = new IbMessageProcessor(messagePublisher);
        KafkaConsumerUtil consumerUtil = new KafkaConsumerUtil(ibMessageProcessor, kafkaConfiguration);
        Consumer<String, String> consumer = consumerUtil.createConsumer();
        consumerUtil.runConsumer(consumer);

    }


    //Creating consumer using required kafka configuration
    private Consumer<String, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaConfiguration.getConsumerBootstrapServerUrl());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                kafkaConfiguration.getConsumerGroup());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConfiguration.getConsumerOffsetResetFlag());

        // Create the consumer using props.
        final Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.<String>singletonList(kafkaConfiguration.getConsumerTopic()));
        return consumer;
    }

    //Starting the consumer
    private void runConsumer(Consumer<String, String> consumer) {
        final int giveUp = 100;   int noRecordsCount = 0;

        //Runs infinitely to read the messages from kafka inbound topic
        while (true) {

            final ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(Duration.ofMillis(kafkaConfiguration.getConsumerPollDuration()));
            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record -> {
                /*System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());*/
                try {
                    //Message is fetched from the topic and passed to message processor to process the message
                    ibMessageProcessor.readMessage(record.value());
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            });

            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }

}
