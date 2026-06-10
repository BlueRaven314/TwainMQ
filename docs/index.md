# TwainMQ

**TwainMQ** is a lightweight, file‑based message log inspired by Kafka’s design principles but without the operational overhead of running a broker or cluster. It provides durable, append‑only topics, partitioned message streams, consumer groups with load balancing, and offset management — all implemented in pure Python.

TwainMQ is designed for:

- local development workflows  
- embedded systems  
- single‑machine (or small local network) data pipelines  
- teaching and experimentation with log‑based messaging  
- applications that need Kafka‑like semantics without a server  

It requires **no services**, **no central authority**, just a directory where the files will live.  All coordination is done via the log files.

---

## Features

- **No running services or brokers**, just an API and distributed self-organisation
- **File‑based topics** stored as append‑only logs
- **Partitions** for parallelism and ordering guarantees
- **Consumer groups** with automatic load balancing
- **Offset tracking and commits**
- **Typed messages** (supports dataclasses)
- **Simple API** for producers and consumers

---

## Important differences from other message brokers

If you are used to other message queues (especially Kafka), then much will seem familiar but there are a few key places where things are different that are worth knowing about up front

- *Consumers always round robin their paritions.*  In kafka if you are behind you might get given a big block of messages from one partition while making no progress on another.  TwainMQ always rotates around the partitions providing the next from each queue in turn if they have any.
- *Rebalances are cheap (often free).*  Rebalances can be called by anyway, and should be called fairly frequently to check all you consumers are still alive.  If there is nothing wrong a rebal is a no-op.  Even when a rebalance does more partitions, priority is given to retaining your existing partitions.  Rebalances do not pause message consumption.
- *Messages must not exceed 4kb.*  Future work will remove this restriction, but for now each message must not exceed 4kb onces compressed and encoded.
- *Keys must be integers.*  Future work will remove this restriction by implementing a key lookup table (so the true keys will still be integers), however for now you can only use integer keys.

---

## Quick Example

```python
from twainmq import TwainMQ

tmq = TwainMQ("/tmp/twain")

# Create a topic
tmq.create_topic("events")

# Write a message
producer = tmq.producer("events")
producer.write_message(42, "hello world")

# Read messages
consumer = tmq.consumer("events")
while message := consumer.poll():
    print(message)
```

---

## Documentation

Use the navigation sidebar to explore:

- **Getting Started** — installation and your first pipeline  
- **Tutorials** — practical examples and patterns  
- **Architecture** — how TwainMQ works internally  
- **API Reference** — auto‑generated from docstrings  

---

## Source Code

TwainMQ is open source and available on GitHub:

<https://github.com/BlueRaven314/TwainMQ>
