package io.bullmq;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper used to derive BullMQ compatible Redis keys.
 */
public class QueueKeys {
    private final String prefix;

    public QueueKeys(String prefix) {
        this.prefix = prefix;
    }

    public Map<String, String> getKeys(String name) {
        String[] names = new String[] {
            "", "active", "wait", "waiting-children", "paused", "completed",
            "failed", "delayed", "repeat", "stalled", "limiter", "prioritized",
            "id", "stalled-check", "meta", "pc", "events", "marker"
        };
        Map<String, String> keys = new HashMap<>();
        for (String type : names) {
            keys.put(type, toKey(name, type));
        }
        return keys;
    }

    public String toKey(String name, String type) {
        return getQueueQualifiedName(name) + ":" + type;
    }

    public String getQueueQualifiedName(String name) {
        return prefix + ":" + name;
    }

    public String getJobKey(String queueName, String jobId) {
        return toKey(queueName, jobId);
    }
}
