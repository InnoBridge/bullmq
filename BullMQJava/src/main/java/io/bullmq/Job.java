package io.bullmq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.bullmq.options.JobOptions;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Representation of a job stored in Redis.
 */
public class Job {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String queueName;
    private final String id;
    private final String name;
    private final Map<String, Object> data;
    private final JobOptions options;
    private final long timestamp;
    private long processedOn;
    private long finishedOn;
    private Object returnValue;
    private String failedReason;

    public Job(String queueName, String id, String name, Map<String, Object> data, JobOptions options, long timestamp) {
        this.queueName = queueName;
        this.id = id;
        this.name = name;
        this.data = data;
        this.options = options;
        this.timestamp = timestamp;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public JobOptions getOptions() {
        return options;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getProcessedOn() {
        return processedOn;
    }

    public void markProcessing() {
        this.processedOn = Instant.now().toEpochMilli();
    }

    public void markFinished(Object value) {
        this.returnValue = value;
        this.finishedOn = Instant.now().toEpochMilli();
        this.failedReason = null;
    }

    public void markFailed(String reason) {
        this.failedReason = reason;
        this.finishedOn = Instant.now().toEpochMilli();
    }

    public Optional<Object> getReturnValue() {
        return Optional.ofNullable(returnValue);
    }

    public Optional<String> getFailedReason() {
        return Optional.ofNullable(failedReason);
    }

    public long getFinishedOn() {
        return finishedOn;
    }

    public String toJson() {
        try {
            return OBJECT_MAPPER.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to serialize job data", e);
        }
    }

    @Override
    public String toString() {
        return "Job{" +
            "queue='" + queueName + '\'' +
            ", id='" + id + '\'' +
            ", name='" + name + '\'' +
            ", data=" + data +
            ", options=" + options +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Job job)) return false;
        return Objects.equals(id, job.id) && Objects.equals(queueName, job.queueName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queueName, id);
    }
}
