# TwainMQ

**TwainMQ** is a lightweight, file‑based message log inspired by Kafka’s design principles — but without the operational overhead of running a broker, cluster, or background service. It provides durable append‑only topics, partitioned message streams, consumer groups with load balancing, and offset management, all coordinated through plain files.

TwainMQ is designed for:

- local development workflows  
- embedded systems  
- single‑machine or small‑network data pipelines  
- teaching and experimentation with log‑based messaging  
- applications that need Kafka‑like semantics without a server  

It requires **no running services**, **no central authority**, and **no background processes**. Everything happens through simple, atomic file operations.

---

## Features

- **Broker‑free operation** — no servers, daemons, or processes to run  
- **Append‑only file‑based topics**  
- **Partitions** for parallelism and ordering guarantees  
- **Consumer groups** with automatic load balancing  
- **Offset tracking and commits**  
- **Typed messages** (supports dataclasses)  
- **Simple, Pythonic API** for producers and consumers  

---

## Important Differences from Other Message Brokers

If you’re familiar with Kafka or similar systems, TwainMQ will feel familiar — but a few behaviours are intentionally different:

- **Consumers always round‑robin their partitions.**  
  Kafka may deliver large batches from one partition while others stall. TwainMQ always rotates partitions, delivering the next available message from each in turn.

- **Rebalances are cheap (often free).**  
  Any consumer may trigger a rebalance at any time. If all consumers are healthy, the rebalance is a no‑op. Even when partitions move, TwainMQ prioritises keeping your existing assignments. Rebalances never pause consumption.

- **Messages must not exceed ~4 KB.**  
  This limit applies after compression and encoding. Future versions will lift this restriction.

- **Keys are fixed width.**  
  Future versions will support arbitrary keys via a lookup table, but currently keys must be integer‑or fixed width string.

---

## Quick Example

```python
from twainmq import Twain

tmq = Twain("/tmp/twain")

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

## Compatible File Systems

TwainMQ relies on being able to perform atomic writes, and so it might not be compatible with all file systems.  Over time I hope to test and add support for more file systems.  This is the current status:

 - **NTFS** - supported, well tested, efficient
 - **XFS** - supported, tested, efficient
 - **ext4** - supported, untested, expected to work
 - **BeeGFS** - supported, but some performance issues (hopefully being resolved soon)
 - **Other POSIX** - generally supported, but with variable performance outcomes
 - **S3** - not supported

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
