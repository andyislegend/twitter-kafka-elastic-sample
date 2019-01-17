package com.edu.kafka.twitterconsumer.outbound;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;

@Slf4j
public class KafkaToElasticConsumer {

    public static void pollAndSend(KafkaConsumerHelper consumerHelper, ElasticClient client) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerHelper.bootstrapProperties())) {
            consumer.subscribe(Collections.singleton(consumerHelper.getTopicName()));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2500));
                int receivedRecords = records.count();
                log.info("Received " + receivedRecords + " records from Kafka");
                BulkRequest bulkRequest = new BulkRequest();
                records.forEach(consumerRecord -> {
                    try {
                        String traceId = consumerHelper.extractTraceId(consumerRecord.value());
                        client.addIndexRequestToBulk(consumerRecord.value(), traceId, bulkRequest);
                    } catch (Exception e) {
                        log.warn("Error while parsing. Skipping bad data...");
                    }
                });
                if (receivedRecords > 0) {
                    try {
                        client.sendSyncBulkRequestToELK(bulkRequest);
                        log.info("Successfully send " + receivedRecords + " records to ELK. Committing offsets");
                        consumer.commitSync();
                    } catch (IOException ex) {
                        log.error("Error while bulk insert, offsets wont be committed!", ex);
                    }
                }
            }
        }
    }
}
