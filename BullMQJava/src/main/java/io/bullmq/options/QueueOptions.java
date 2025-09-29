package io.bullmq.options;

/**
 * Connection and queue level options.
 */
public class QueueOptions {
    private String host = "127.0.0.1";
    private int port = 6379;
    private int database = 0;
    private String prefix = "bull";

    public String getHost() {
        return host;
    }

    public QueueOptions setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public QueueOptions setPort(int port) {
        this.port = port;
        return this;
    }

    public int getDatabase() {
        return database;
    }

    public QueueOptions setDatabase(int database) {
        this.database = database;
        return this;
    }

    public String getPrefix() {
        return prefix;
    }

    public QueueOptions setPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }
}
