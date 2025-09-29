# BullMQ Java (experimental)

This module contains a lightweight Java implementation of the core BullMQ
concepts. It provides a queue abstraction backed by Redis and a worker that can
process jobs using the same key structure as the official BullMQ project. The
implementation focuses on being easy to integrate in existing JVM applications
rather than feature parity with the Node.js or Python versions.

## Features

- Add single jobs or batches to a queue
- Retrieve queue statistics (waiting/active/completed/failed counts)
- Obliterate a queue and its data from Redis
- Background worker that continuously consumes jobs
- Maven project that targets the latest stable Java release (Java 21)

## Quick start

```
mvn -f BullMQJava/pom.xml test
```

> **Note**
> The build downloads Maven plugins and dependencies from Maven Central. If
> your environment proxies or blocks access to https://repo.maven.apache.org
> you may see `403 Forbidden` errors during the first run. Point Maven at an
> accessible mirror (for example by adding a `<mirror>` entry to your
> `~/.m2/settings.xml`) or populate a local repository before running the
> build.

Add the dependency to your own Maven project by installing the artifact locally
and referencing it as `io.bullmq:bullmq-java`.

### Example

```java
QueueOptions options = new QueueOptions()
    .setHost("localhost")
    .setPort(6379);

try (Queue queue = new Queue("email", options)) {
    Map<String, Object> data = Map.of("userId", 123, "template", "welcome");
    queue.add("send", data);
}
```

To process jobs:

```java
QueueOptions options = new QueueOptions();
Worker worker = new Worker("email", job -> {
    System.out.println("Processing job " + job.getId() + " with data: " + job.getData());
    return "ok";
}, options);
worker.start();
```

## Limitations

- Delayed, repeatable and flow jobs are not supported yet.
- Only a subset of the metadata stored by the Node.js implementation is
  persisted.
- The worker currently executes the processor synchronously on a single thread.

Pull requests are welcome!
