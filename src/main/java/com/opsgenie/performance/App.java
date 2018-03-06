package com.opsgenie.performance;

public class App {

    public static void main(String[] args) {
        RedisClientReadWritePerformanceTester tester = new RedisClientReadWritePerformanceTester();

        try {
            tester.initializeConnections();
            tester.runReadWriteTests();
            tester.closeConnections();
        } finally {
            tester.shutdownClients();
        }
    }
}
