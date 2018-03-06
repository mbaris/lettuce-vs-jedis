package com.opsgenie.performance;

import java.util.UUID;

public class RedisClientLoadTest implements Runnable {

    private RedisGetOperation getOperation;
    private RedisSetOperation setOperation;
    private int totalNumberOfOperations = 100000;


    RedisClientLoadTest(RedisGetOperation getOperation, RedisSetOperation setOperation, int totalNumberOfOperations) {
        this.getOperation = getOperation;
        this.setOperation = setOperation;
        this.totalNumberOfOperations = totalNumberOfOperations;
    }

    @Override
    public void run() {
        for (int n = 0; n <= totalNumberOfOperations; n++) {
            String key = UUID.randomUUID().toString();
            final String value = UUID.randomUUID().toString();
            setOperation.set(key, value);
            final String s = getOperation.get(key);
            if (!s.equals(value)) {
                throw new IllegalStateException("This should not happen");
            }

        }
    }

    @FunctionalInterface
    interface RedisGetOperation {
        String get(String key);
    }

    @FunctionalInterface
    interface RedisSetOperation {
        void set(String key, String value);
    }

}
