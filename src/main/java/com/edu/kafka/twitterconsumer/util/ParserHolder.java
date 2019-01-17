package com.edu.kafka.twitterconsumer.util;

import com.google.gson.JsonParser;

import java.util.concurrent.locks.ReentrantLock;

public final class ParserHolder {

    private static JsonParser _instance;
    private static final ReentrantLock lock = new ReentrantLock();

    public static JsonParser getParser() {
        if (null == _instance) {
            lock.lock();
            try {
                if (null == _instance) {
                    _instance = new JsonParser();
                }
            } finally {
                lock.unlock();
            }
        }
        return _instance;
    }
}
