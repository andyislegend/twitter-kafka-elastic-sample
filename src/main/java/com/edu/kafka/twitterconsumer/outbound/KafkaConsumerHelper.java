package com.edu.kafka.twitterconsumer.outbound;

import com.edu.kafka.twitterconsumer.util.ParserHolder;
import com.google.gson.JsonParser;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaConsumerHelper {

    private final JsonParser parser;

    @Getter
    private String topicName;
    @Getter
    private String kafkaLocation;
    @Getter
    private String groupId;

    public KafkaConsumerHelper(String topicName, String kafkaLocation, String groupId) {
        this.topicName = topicName;
        this.parser = ParserHolder.getParser();
        this.kafkaLocation = kafkaLocation;
        this.groupId = groupId;
    }

    public Properties bootstrapProperties() {
        String stringDeserializerClassName = StringDeserializer.class.getName();

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaLocation);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, stringDeserializerClassName);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, stringDeserializerClassName);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        return properties;
    }

    public String extractTraceId(String value) {
        return this.parser.parse(value).getAsJsonObject().get("id_str").getAsString();
    }
}
