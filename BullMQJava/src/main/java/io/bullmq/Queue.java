package io.bullmq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.bullmq.options.JobOptions;
import io.bullmq.options.QueueOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Minimal queue API that reuses BullMQ's Redis key layout.
 */
public class Queue implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Queue.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final String name;
    private final QueueOptions options;
    private final RedisConnection connection;
    private final UnifiedJedis client;
    private final QueueKeys queueKeys;
    private final Map<String, String> keys;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public Queue(String name) {
        this(name, new QueueOptions());
    }

    public Queue(String name, QueueOptions options) {
        this(name, options, new RedisConnection(options.getHost(), options.getPort(), options.getDatabase()));
    }

    Queue(String name, QueueOptions options, RedisConnection connection) {
        this.name = Objects.requireNonNull(name, "name");
        this.options = options;
        this.queueKeys = new QueueKeys(options.getPrefix());
        this.connection = connection;
        this.client = connection.getClient();
        this.keys = queueKeys.getKeys(name);
        ensureMetadata();
    }

    private void ensureMetadata() {
        client.hsetnx(keys.get("meta"), "version", "java-0.1.0");
        client.hsetnx(keys.get("meta"), "name", name);
    }

    public String getName() {
        return name;
    }

    public QueueOptions getOptions() {
        return options;
    }

    public Job add(String jobName, Map<String, Object> data) {
        return add(jobName, data, new JobOptions());
    }

    public Job add(String jobName, Map<String, Object> data, JobOptions jobOptions) {
        ensureOpen();
        Map<String, Object> payload = new HashMap<>(data);
        JobOptions effectiveOptions = jobOptions == null ? new JobOptions() : jobOptions;
        String jobId = effectiveOptions.getJobId().orElseGet(() -> nextJobId());
        long timestamp = effectiveOptions.getTimestamp();
        Job job = new Job(name, jobId, jobName, payload, effectiveOptions, timestamp);

        Map<String, String> values = new HashMap<>();
        values.put("name", jobName);
        values.put("data", job.toJson());
        values.put("opts", writeJson(Map.of(
            "timestamp", timestamp,
            "delay", effectiveOptions.getDelay(),
            "attempts", effectiveOptions.getAttempts()
        )));
        values.put("timestamp", Long.toString(timestamp));
        String jobKey = queueKeys.getJobKey(name, jobId);
        client.hset(jobKey, values);
        client.rpush(keys.get("wait"), jobId);
        if (effectiveOptions.getDelay() > 0) {
            client.zadd(keys.get("delayed"), timestamp + effectiveOptions.getDelay(), jobId);
        }
        client.hset(keys.get("meta"), "updated", Long.toString(Instant.now().toEpochMilli()));
        LOGGER.debug("Added job {} to queue {}", jobId, name);
        return job;
    }

    public List<Job> addBulk(List<BulkJob> jobs) {
        ensureOpen();
        List<Job> created = new ArrayList<>();
        for (BulkJob bulk : jobs) {
            JobOptions options = Optional.ofNullable(bulk.options()).orElseGet(JobOptions::new);
            created.add(add(bulk.name(), bulk.data(), options));
        }
        return created;
    }

    public void obliterate() {
        ensureOpen();
        deleteByPattern(queueKeys.getQueueQualifiedName(name) + ":*");
    }

    private void deleteByPattern(String pattern) {
        String cursor = ScanParams.SCAN_POINTER_START;
        ScanParams params = new ScanParams().match(pattern).count(200);
        do {
            ScanResult<String> result = client.scan(cursor, params);
            List<String> keys = result.getResult();
            if (!keys.isEmpty()) {
                client.del(keys.toArray(new String[0]));
            }
            cursor = result.getCursor();
        } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
    }

    private String nextJobId() {
        return Long.toString(client.incr(keys.get("id")));
    }

    private String writeJson(Object value) {
        try {
            return OBJECT_MAPPER.writeValueAsString(value);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to serialize value", e);
        }
    }

    public Optional<Job> getJob(String jobId) {
        ensureOpen();
        String jobKey = queueKeys.getJobKey(name, jobId);
        Map<String, String> hash = client.hgetAll(jobKey);
        if (hash == null || hash.isEmpty()) {
            return Optional.empty();
        }
        try {
            Map<String, Object> data = OBJECT_MAPPER.readValue(hash.getOrDefault("data", "{}"), MAP_TYPE);
            JobOptions options = new JobOptions();
            options.setJobId(jobId);
            return Optional.of(new Job(name, jobId, hash.get("name"), data, options,
                Long.parseLong(hash.getOrDefault("timestamp", Long.toString(System.currentTimeMillis())))));
        } catch (IOException e) {
            throw new IllegalStateException("Unable to deserialize job", e);
        }
    }

    public long getWaitingCount() {
        ensureOpen();
        return client.llen(keys.get("wait"));
    }

    public long getActiveCount() {
        ensureOpen();
        return client.llen(keys.get("active"));
    }

    public long getCompletedCount() {
        ensureOpen();
        return client.zcard(keys.get("completed"));
    }

    public long getFailedCount() {
        ensureOpen();
        return client.zcard(keys.get("failed"));
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Queue already closed");
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            connection.close();
        }
    }

    public record BulkJob(String name, Map<String, Object> data, JobOptions options) { }
}
