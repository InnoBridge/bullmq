package io.bullmq;

import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.bullmq.options.JobOptions;
import io.bullmq.options.QueueOptions;
import io.bullmq.worker.Worker;

import static org.junit.jupiter.api.Assertions.*;

class QueueTest {
    private RedisServer server;

    @BeforeEach
    void startServer() throws Exception {
        server = RedisServer.newRedisServer().start();
    }

    @AfterEach
    void stopServer() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void addsAndRetrievesJob() throws Exception {
        Jedis jedis = new Jedis(server.getHost(), server.getBindPort());
        QueueOptions options = new QueueOptions().setPrefix("test");
        try (Queue queue = new Queue("emails", options, new RedisConnection(jedis, () -> {}))) {
            Job job = queue.add("send", Map.of("userId", 123));
            assertNotNull(job.getId());
            assertEquals(1, queue.getWaitingCount());

            JobOptions opts = new JobOptions();
            List<Job> jobs = queue.addBulk(List.of(new Queue.BulkJob("send", Map.of("userId", 456), opts)));
            assertEquals(1, jobs.size());
            assertEquals(2, queue.getWaitingCount());

            Job stored = queue.getJob(job.getId()).orElseThrow();
            assertEquals("send", stored.getName());
            assertEquals(123, stored.getData().get("userId"));
        } finally {
            jedis.close();
        }
    }

    @Test
    void workerProcessesJobs() throws Exception {
        Jedis producerJedis = new Jedis(server.getHost(), server.getBindPort());
        Jedis workerJedis = new Jedis(server.getHost(), server.getBindPort());
        QueueOptions options = new QueueOptions().setPrefix("test");

        CountDownLatch latch = new CountDownLatch(1);
        try (Queue queue = new Queue("reports", options, new RedisConnection(producerJedis, () -> {}));
             Worker worker = new Worker("reports", job -> {
                 latch.countDown();
                 return "ok";
             }, options, java.time.Duration.ofMillis(200), new RedisConnection(workerJedis, () -> {}))) {
            worker.start();
            queue.add("generate", Map.of("id", 1));
            assertTrue(latch.await(2, TimeUnit.SECONDS));
            assertEquals(0, queue.getWaitingCount());
        } finally {
            producerJedis.close();
            workerJedis.close();
        }
    }
}
