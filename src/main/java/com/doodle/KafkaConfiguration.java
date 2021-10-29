package com.doodle;

import java.util.Properties;

/**
 * Configuration class to extract all Properties file keys
 */
public class KafkaConfiguration {

    private String producerBootstrapServerUrl;
    private String producerTopic;
    private String consumerBootstrapServerUrl;
    private String consumerTopic;
    private String consumerGroup;
    private String consumerOffsetResetFlag;
    private Long consumerPollDuration;

    public void loadConfig(Properties properties){
        this.setProducerTopic(properties.getProperty("producer.topic"));
        this.setProducerBootstrapServerUrl(properties.getProperty("producer.bootstrap.server.url", "localhost:9092"));

        this.setConsumerBootstrapServerUrl(properties.getProperty("consumer.bootstrap.server.url", "localhost:9092"));
        this.setConsumerTopic(properties.getProperty("consumer.topic"));
        this.setConsumerGroup(properties.getProperty("consumer.group"));
        this.setConsumerPollDuration(Long.valueOf(properties.getProperty("consumer.poll.duration", "1000")));
        this.setConsumerOffsetResetFlag(properties.getProperty("consumer.offset.reset.flag", "earliest"));
    }
    public String getProducerBootstrapServerUrl() {
        return producerBootstrapServerUrl;
    }

    public void setProducerBootstrapServerUrl(String producerBootstrapServerUrl) {
        this.producerBootstrapServerUrl = producerBootstrapServerUrl;
    }

    public String getProducerTopic() {
        return producerTopic;
    }

    public void setProducerTopic(String producerTopic) {
        this.producerTopic = producerTopic;
    }

    public String getConsumerBootstrapServerUrl() {
        return consumerBootstrapServerUrl;
    }

    public void setConsumerBootstrapServerUrl(String consumerBootstrapServerUrl) {
        this.consumerBootstrapServerUrl = consumerBootstrapServerUrl;
    }

    public String getConsumerTopic() {
        return consumerTopic;
    }

    public void setConsumerTopic(String consumerTopic) {
        this.consumerTopic = consumerTopic;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getConsumerOffsetResetFlag() {
        return consumerOffsetResetFlag;
    }

    public void setConsumerOffsetResetFlag(String consumerOffsetResetFlag) {
        this.consumerOffsetResetFlag = consumerOffsetResetFlag;
    }

    public Long getConsumerPollDuration() {
        return consumerPollDuration;
    }

    public void setConsumerPollDuration(Long consumerPollDuration) {
        this.consumerPollDuration = consumerPollDuration;
    }
}
