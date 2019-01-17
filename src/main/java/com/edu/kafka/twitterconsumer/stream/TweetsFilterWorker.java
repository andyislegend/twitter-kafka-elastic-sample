package com.edu.kafka.twitterconsumer.stream;

import com.edu.kafka.twitterconsumer.util.ParserHolder;
import com.edu.kafka.twitterconsumer.util.TopicName;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class TweetsFilterWorker {

    private static final ReentrantLock lock = new ReentrantLock();
    private static TweetsFilterWorker _instance;
    private final AtomicBoolean processStarted = new AtomicBoolean(false);

    public static TweetsFilterWorker getFilterInstance() {
        if (null == _instance) {
            lock.lock();
            try {
                if (null == _instance) {
                    _instance = new TweetsFilterWorker();
                }
            } finally {
                lock.unlock();
            }
        }
        return _instance;
    }

    private static int extractUserFollowers(String tweet, JsonParser parser) {
        try {
            return parser.parse(tweet).getAsJsonObject().get("user").getAsJsonObject().get("followers_count").getAsInt();
        } catch (JsonSyntaxException e) {
            return 0;
        }
    }

    public StreamsBuilder createTopology() {
        JsonParser parser = ParserHolder.getParser();
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> inputTopic = streamsBuilder.stream(TopicName.INPUT);
        inputTopic.filter((k, tweetBody) ->
                extractUserFollowers(tweetBody, parser) > 100
        ).to(TopicName.FILTERED);
        return streamsBuilder;
    }

    public void start(StreamsBuilder topology, Properties streamProperties) {
        if (processStarted.compareAndSet(false, true)) {
            //build topology
            KafkaStreams kafkaStreams = new KafkaStreams(topology.build(), streamProperties);

            //add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

            //start streaming and filtering
            kafkaStreams.start();
        } else {
            log.warn("streaming process already have been started elsewhere");
        }
    }
}
