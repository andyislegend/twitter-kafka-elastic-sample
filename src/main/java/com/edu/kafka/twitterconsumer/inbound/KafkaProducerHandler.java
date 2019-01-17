package com.edu.kafka.twitterconsumer.inbound;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class KafkaProducerHandler {

    @Getter
    private final KafkaProducer<String, String> kafkaProducer;

    public KafkaProducerHandler(String kafkaLocation) {
        this.kafkaProducer = createKafkaProducer(kafkaLocation);
    }

    public KafkaProducer<String, String> createKafkaProducer(String kafkaLocation) {
        String stringSerializerClassName = StringSerializer.class.getName();
        //Config properties for kafka producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaLocation);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, stringSerializerClassName);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, stringSerializerClassName);
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.TRUE.toString());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Integer.toString(5));
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "30");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return new KafkaProducer<>(properties);
    }
}
