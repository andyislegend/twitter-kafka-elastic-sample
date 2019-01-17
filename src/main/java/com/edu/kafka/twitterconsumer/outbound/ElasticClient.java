package com.edu.kafka.twitterconsumer.outbound;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class ElasticClient {

    private static final ReentrantLock lock = new ReentrantLock();
    private static ElasticClient _instance;
    private final RestHighLevelClient client;

    private ElasticClient(RestHighLevelClient client) {
        this.client = client;
    }

    public static ElasticClient getRestELKClient() {
        if (null == _instance) {
            lock.lock();
            try {
                if (null == _instance) {
                    _instance = new ElasticClient(bootstrapClient());
                }
            } finally {
                lock.unlock();
            }
        }
        return _instance;
    }

    private static RestHighLevelClient bootstrapClient() {

        String username = "";
        String password = "";
        String hostname = "";

        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder clientBuilder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback((httpAsyncClientBuilder) ->
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credsProvider)
                );

        return new RestHighLevelClient(clientBuilder);
    }

    public void sendSyncBulkRequestToELK(BulkRequest request) throws IOException {
        this.client.bulk(request, RequestOptions.DEFAULT);
    }

    public void addIndexRequestToBulk(String source, String traceId, BulkRequest bulkRequest) {
        IndexRequest indexRequest = new IndexRequest("twitter", "tweets", traceId)
                .source(source, XContentType.JSON);
        bulkRequest.add(indexRequest);
    }
}
