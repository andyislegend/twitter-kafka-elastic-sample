package com.edu.kafka.twitterconsumer.inbound;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.Getter;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

public class TwitterClient {

    private static final String CONSUMER_KEY = "";
    private static final String CONSUMER_SEC = "";
    private static final String ACCESS_TOKEN = "";
    private static final String ACCESS_SECRET = "";
    private static final String CLIENT_APP_ID_LOGS = "ipreferespreso-java-client-01";
    private static final ReentrantLock lock = new ReentrantLock();

    private static TwitterClient _instance;

    @Getter
    private BlockingQueue<String> msgQueue;
    @Getter
    private Client client;

    private TwitterClient(List<String> termsToTrack) {
        this.msgQueue = new LinkedBlockingQueue<>(1000);
        this.client = setUpClient(this.msgQueue, termsToTrack);
    }

    public static TwitterClient getTweeterClient(List<String> termsToTrack) {
        if (null == _instance) {
            lock.lock();
            try {
                if (null == _instance) {
                    _instance = new TwitterClient(termsToTrack);
                }
            } finally {
                lock.unlock();
            }
        }
        return _instance;
    }

    private Client setUpClient(BlockingQueue<String> msgQueue, List<String> termsToTrack) {
        Hosts hsbHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hsdEndpoint = new StatusesFilterEndpoint()
                .trackTerms(Lists.newArrayList(termsToTrack));

        Authentication hsbAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SEC, ACCESS_TOKEN, ACCESS_SECRET);

        return new ClientBuilder()
                .name(CLIENT_APP_ID_LOGS) // optional: mainly for the logs
                .hosts(hsbHosts)
                .authentication(hsbAuth)
                .endpoint(hsdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .build();
    }
}
