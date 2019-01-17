package com.edu.kafka.twitterconsumer.inbound;

import com.twitter.hbc.core.Client;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TwitterProducer {

    public void produce(TwitterClient twitterClient, KafkaProducer<String, String> producer, String topic) {

        Client hsbClient = twitterClient.getClient();
        hsbClient.connect();

        try {
            while (!hsbClient.isDone()) {
                Optional<String> msg = Optional.ofNullable(twitterClient
                        .getMsgQueue()
                        .poll(100, TimeUnit.MILLISECONDS));
                msg.ifPresent(value ->
                        producer.send(new ProducerRecord<>(topic, null, value),
                                (metadata, exception) -> {
                                    if (null == exception) {
                                        log.info("========> : Send to topic: " + metadata.topic()
                                                + ", partition: " + metadata.partition());
                                    } else {
                                        log.error("Error happened", exception);
                                    }
                                })
                );
            }
        } catch (InterruptedException e) {
            log.error("Interruption exception occurred: ", e);
        } finally {
            hsbClient.stop();
        }
    }
}
