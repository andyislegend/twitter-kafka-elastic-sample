package com.edu.kafka.twitterconsumer;

import com.edu.kafka.twitterconsumer.outbound.ElasticClient;
import com.edu.kafka.twitterconsumer.outbound.KafkaConsumerHelper;
import com.edu.kafka.twitterconsumer.outbound.KafkaToElasticConsumer;
import com.edu.kafka.twitterconsumer.util.TopicName;

public class KafkaToElasticProcessorStarter {

    public static void main(String[] args) {
        //create kafka consumer helper (place where consumer configuration is coming from)
        KafkaConsumerHelper helper = new KafkaConsumerHelper(
                TopicName.FILTERED,
                "localhost:9092",
                "kafka-twitter-consumer");

        //start polling Kafka and sending obtained data to Elastic
        KafkaToElasticConsumer.pollAndSend(helper, ElasticClient.getRestELKClient());
    }
}
