package com.edu.kafka.twitterconsumer;

import com.edu.kafka.twitterconsumer.stream.KafkaStreamPropsHolder;
import com.edu.kafka.twitterconsumer.stream.TweetsFilterWorker;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Properties;

public class KafkaStreamFilterStarter {

    public static void main(String[] args) {
        Properties streamProperties = KafkaStreamPropsHolder
                .getStreamProperties("localhost:9092", "kafka-twitter-stream");
        TweetsFilterWorker tweetsFilterWorker = TweetsFilterWorker.getFilterInstance();
        StreamsBuilder topology = tweetsFilterWorker.createTopology();
        tweetsFilterWorker.start(topology, streamProperties);
    }
}
