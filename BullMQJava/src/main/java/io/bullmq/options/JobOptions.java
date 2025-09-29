package io.bullmq.options;

import java.util.Optional;

/**
 * Options that influence job creation.
 */
public class JobOptions {
    private String jobId;
    private long timestamp = System.currentTimeMillis();
    private long delay = 0L;
    private int attempts = 1;

    public Optional<String> getJobId() {
        return Optional.ofNullable(jobId);
    }

    public JobOptions setJobId(String jobId) {
        this.jobId = jobId;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public JobOptions setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public long getDelay() {
        return delay;
    }

    public JobOptions setDelay(long delay) {
        this.delay = delay;
        return this;
    }

    public int getAttempts() {
        return attempts;
    }

    public JobOptions setAttempts(int attempts) {
        this.attempts = attempts;
        return this;
    }
}
