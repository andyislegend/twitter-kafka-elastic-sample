package com.edu.kafka.twitterconsumer;

import com.edu.kafka.twitterconsumer.inbound.KafkaProducerHandler;
import com.edu.kafka.twitterconsumer.inbound.TwitterClient;
import com.edu.kafka.twitterconsumer.inbound.TwitterProducer;
import com.edu.kafka.twitterconsumer.util.TopicName;

import java.util.Arrays;

public class TweeterToKafkaProcessorStarter {

    public static void main(String[] args) {

        // setting up a twitter-to-kafka producer
        TwitterClient twitterClient = TwitterClient.getTweeterClient(Arrays.asList("dota", "dota2"));
        KafkaProducerHandler kafkaProducerHandler = new KafkaProducerHandler("localhost:9092");

        //shutdown hooks to close everything gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            twitterClient.getClient().stop();
            kafkaProducerHandler.getKafkaProducer().close();
        }));

        //start streaming tweets to Kafka
        new TwitterProducer().produce(twitterClient, kafkaProducerHandler.getKafkaProducer(), TopicName.INPUT);
    }
}
