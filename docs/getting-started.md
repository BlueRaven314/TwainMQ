# Getting Started

This guide walks you through installing TwainMQ, creating your first topic, producing messages, and consuming them with a consumer group.

---

## Installation

TwainMQ is not yet published on PyPI, so install it directly from the repository:

```bash
pip install git+https://github.com/BlueRaven314/TwainMQ.git
```

Or clone the repo for development:

```bash
git clone https://github.com/BlueRaven314/TwainMQ.git
cd TwainMQ
pip install -e .
```

---

## Creating a Message Store

TwainMQ stores all data in a directory you choose:

```python
from twainmq import TwainMQ

mq = TwainMQ("/tmp/twain")
```

This directory will contain:

- topic folders  
- partition files  
- offset tracking  
- consumer group metadata  

Everything is plain files — easy to inspect and debug.

---

## Create a topic

A topic must be created before you can publish to it

```python
from twainmq import TwainMQ

tmq = TwainMQ("/tmp/twain")

tmq.create_topic("events", key_type = "u16", partitions = 4)
```

This will create a topic called "events" where messages are split between 4 partitions.  All messages must be published with a key, and this topic will use `u16` type keys (which is the default anyway).  We have not specified any message types, so this topic will be for sending strings (byte strings or utf-8).

---

## Producing Messages

Create a producer for a topic and send messages:

```python
producer = tmq.producer("events")

producer.send(12, "Hello world")

x = 12345
producer.send(42, x.to_bytes(16))
```

Messages are appended to the topic’s log file.

---

## Consuming Messages

Consumers read from topics and track their own offsets:

```python
consumer = mq.consumer("events", group="event-readers")

print(consumer.poll())
consumer.commit()
print(consumer.poll())

```

### What happens here?

- The consumer joins the **event-readers** consumer group, creating it if it doesn't already exist
- The consumer triggers a rebalance event for the group, and offers to accept some partitions
- At the end of the rebalance period (default 60 seconds) partitions are confirmed with each consumer (in our case only 1 consumer, so it will get all of them)
- Polling returns the next message for that consumer (round robin across the partitions it owns)
- 

---

## Consumer Groups

Multiple consumers in the same group share the work:

```python
c1 = mq.consumer("events", group="workers")
c2 = mq.consumer("events", group="workers")
```

TwainMQ will:

- detect both consumers  
- rebalance partitions  
- ensure each partition is processed by exactly one consumer  

This mirrors Kafka’s consumer group semantics — but locally.

---

## Next Steps

Check out:

- **Tutorials** for real‑world examples  
- **Architecture** to understand how TwainMQ works internally  
- **API Reference** for details on every class and method  

TwainMQ is intentionally small and readable — feel free to explore the source code and adapt it to your needs.
