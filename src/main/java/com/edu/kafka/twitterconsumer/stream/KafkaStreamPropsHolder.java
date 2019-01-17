package com.edu.kafka.twitterconsumer.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public final class KafkaStreamPropsHolder {

    public static Properties getStreamProperties(String kafkaLocation, String appId) {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaLocation);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        return properties;
    }
}
