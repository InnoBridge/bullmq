package io.bullmq.worker;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.bullmq.Job;
import io.bullmq.QueueKeys;
import io.bullmq.RedisConnection;
import io.bullmq.options.JobOptions;
import io.bullmq.options.QueueOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.UnifiedJedis;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Background worker that continuously consumes jobs from a queue.
 */
public class Worker implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final String name;
    private final QueueOptions options;
    private final Function<Job, Object> processor;
    private final RedisConnection connection;
    private final UnifiedJedis client;
    private final QueueKeys queueKeys;
    private final Map<String, String> keys;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Duration blockTimeout;

    private Future<?> loopFuture;

    public Worker(String name, Function<Job, Object> processor) {
        this(name, processor, new QueueOptions(), Duration.ofSeconds(5));
    }

    public Worker(String name, Function<Job, Object> processor, QueueOptions options) {
        this(name, processor, options, Duration.ofSeconds(5));
    }

    public Worker(String name, Function<Job, Object> processor, QueueOptions options, Duration blockTimeout) {
        this(name, processor, options, blockTimeout, new RedisConnection(options.getHost(), options.getPort(), options.getDatabase()));
    }

    Worker(String name, Function<Job, Object> processor, QueueOptions options, Duration blockTimeout, RedisConnection connection) {
        this.name = Objects.requireNonNull(name, "name");
        this.options = Objects.requireNonNull(options, "options");
        this.processor = Objects.requireNonNull(processor, "processor");
        this.blockTimeout = Objects.requireNonNull(blockTimeout, "blockTimeout");
        this.queueKeys = new QueueKeys(options.getPrefix());
        this.keys = queueKeys.getKeys(name);
        this.connection = connection;
        this.client = connection.getClient();
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            loopFuture = executor.submit(this::runLoop);
        }
    }

    private void runLoop() {
        LOGGER.info("Worker for queue {} started", name);
        while (running.get()) {
            try {
                String jobId = client.brpoplpush(keys.get("wait"), keys.get("active"), (int) blockTimeout.getSeconds());
                if (jobId == null) {
                    continue;
                }
                processJob(jobId);
            } catch (Exception e) {
                LOGGER.error("Worker loop error", e);
                try {
                    Thread.sleep(250L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        LOGGER.info("Worker for queue {} stopped", name);
    }

    private void processJob(String jobId) {
        String jobKey = queueKeys.getJobKey(name, jobId);
        Map<String, String> hash = client.hgetAll(jobKey);
        if (hash == null || hash.isEmpty()) {
            LOGGER.warn("Received job {} with no data", jobId);
            client.lrem(keys.get("active"), 0, jobId);
            return;
        }
        Job job = hydrateJob(jobId, hash);
        job.markProcessing();
        client.hset(jobKey, Map.of(
            "processedOn", Long.toString(job.getProcessedOn())
        ));

        try {
            Object result = processor.apply(job);
            job.markFinished(result);
            completeJob(job, jobKey);
        } catch (Exception e) {
            job.markFailed(e.getMessage());
            failJob(job, jobKey, e);
        }
    }

    private Job hydrateJob(String jobId, Map<String, String> hash) {
        try {
            Map<String, Object> data = OBJECT_MAPPER.readValue(hash.getOrDefault("data", "{}"), MAP_TYPE);
            JobOptions options = new JobOptions();
            options.setJobId(jobId);
            if (hash.containsKey("timestamp")) {
                options.setTimestamp(Long.parseLong(hash.get("timestamp")));
            }
            return new Job(name, jobId, hash.get("name"), data, options,
                Long.parseLong(hash.getOrDefault("timestamp", Long.toString(Instant.now().toEpochMilli()))));
        } catch (IOException e) {
            throw new IllegalStateException("Unable to hydrate job", e);
        }
    }

    private void completeJob(Job job, String jobKey) {
        client.hset(jobKey, Map.of(
            "returnvalue", job.getReturnValue().map(this::writeJson).orElse("null"),
            "finishedOn", Long.toString(job.getFinishedOn())
        ));
        client.lrem(keys.get("active"), 0, job.getId());
        client.zadd(keys.get("completed"), job.getFinishedOn(), job.getId());
        LOGGER.info("Job {} completed", job.getId());
    }

    private void failJob(Job job, String jobKey, Exception error) {
        client.hset(jobKey, Map.of(
            "failedReason", job.getFailedReason().orElse(error.getMessage()),
            "finishedOn", Long.toString(job.getFinishedOn())
        ));
        client.lrem(keys.get("active"), 0, job.getId());
        client.zadd(keys.get("failed"), job.getFinishedOn(), job.getId());
        LOGGER.warn("Job {} failed", job.getId(), error);
    }

    private String writeJson(Object value) {
        try {
            return OBJECT_MAPPER.writeValueAsString(value);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to serialize value", e);
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            if (loopFuture != null) {
                loopFuture.cancel(true);
            }
            executor.shutdownNow();
        }
    }

    @Override
    public void close() {
        stop();
        connection.close();
    }
}
