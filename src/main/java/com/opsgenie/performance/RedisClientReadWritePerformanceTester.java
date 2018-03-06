package com.opsgenie.performance;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import redis.clients.jedis.Jedis;

import java.util.concurrent.*;
import java.util.stream.IntStream;

public class RedisClientReadWritePerformanceTester {

    private final static int TOTAL_OPERATIONS = 100000;

    private Jedis jedisClient;
    private RedisClient lettuceClient;
    private StatefulRedisConnection<String, String> lettuceConnection;

    public void initializeConnections() {
        jedisClient = new Jedis("localhost");
        lettuceClient = RedisClient.create("redis://localhost");
        lettuceConnection = lettuceClient.connect();
        jedisClient.connect();
    }

    public void runReadWriteTests() {
        jedisClient.flushAll();
        final RedisCommands<String, String> syncLettuce = lettuceConnection.sync();

        System.out.println("----------------------------------------");
        System.out.println("Beginning single thread tests");
        final long singleThreadJedis = calculateElapsedTimeInMillis(() -> testSingleThread(new RedisClientLoadTest(jedisClient::get, jedisClient::set, TOTAL_OPERATIONS)));
        final long singleThreadLettuce = calculateElapsedTimeInMillis(() -> testSingleThread(new RedisClientLoadTest(syncLettuce::get, syncLettuce::set, TOTAL_OPERATIONS)));
        System.out.println(TOTAL_OPERATIONS + "operations,  single thread jedis took: " + singleThreadJedis + " ms");
        System.out.println(TOTAL_OPERATIONS + "operations,  single thread lettuce took: " + singleThreadLettuce + " ms");

        System.out.println("----------------------------------------");
        System.out.println("Beginning two thread tests");
        final long twoThreadsJedis = calculateElapsedTimeInMillis(() -> testMultiThread(2, new RedisClientLoadTest(jedisClient::get, jedisClient::set, TOTAL_OPERATIONS / 2)));
        final long twoThreadsLettuce = calculateElapsedTimeInMillis(() -> testMultiThread(2, new RedisClientLoadTest(syncLettuce::get, syncLettuce::set, TOTAL_OPERATIONS / 2)));
        System.out.println(TOTAL_OPERATIONS + "operations,  two thread jedis took: " + twoThreadsJedis + " ms");
        System.out.println(TOTAL_OPERATIONS + "operations,  two thread lettuce took: " + twoThreadsLettuce + " ms");

        System.out.println("----------------------------------------");
        System.out.println("Beginning four thread tests");
        final long fourThreadsJedis = calculateElapsedTimeInMillis(() -> testMultiThread(4, new RedisClientLoadTest(jedisClient::get, jedisClient::set, TOTAL_OPERATIONS / 4)));
        final long fourThreadsLettuce = calculateElapsedTimeInMillis(() -> testMultiThread(4, new RedisClientLoadTest(syncLettuce::get, syncLettuce::set, TOTAL_OPERATIONS / 4)));
        System.out.println(TOTAL_OPERATIONS + "operations,  four thread jedis took: " + fourThreadsJedis + " ms");
        System.out.println(TOTAL_OPERATIONS + "operations,  four thread lettuce took: " + fourThreadsLettuce + " ms");
    }

    public void shutdownClients() {
        jedisClient.disconnect();
        lettuceClient.shutdown();
    }

    public void closeConnections() {
        jedisClient.close();
        lettuceConnection.close();
    }

    private static long calculateElapsedTimeInMillis(Runnable runnable) {
        final long begin = System.nanoTime();
        runnable.run();
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
    }

    private static void testSingleThread(RedisClientLoadTest redisClientTester) {
        redisClientTester.run();
    }

    private static void testMultiThread(int threadCount, RedisClientLoadTest loadTest) {
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        IntStream.range(0, threadCount).forEach(it -> {
            final Future<?> submit = pool.submit(loadTest);
            try {
                submit.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
        pool.shutdown();
    }

}
