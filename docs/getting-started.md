# Getting Started

This guide walks you through installing TwainMQ, creating your first topic, producing messages, and consuming them — including how consumer groups work.

TwainMQ is intentionally simple: everything is stored as plain files, and producers/consumers are lightweight Python objects. If you know how to work with directories, you can understand TwainMQ.

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
from twainmq import Twain

mq = Twain("/tmp/twain")
```
This directory will contain:

- topic folders  
- partition files  
- consumer-group metadata  
- topic configuration (`.twc` files)

Everything is plain files — easy to inspect, debug, and back up.

---

## Creating a Topic

A topic must be created before you can publish messages to it:

```python
tmq = Twain("/tmp/twain")

tmq.create_topic("events", key_type="u16", partitions=4)
```

This creates a topic named `"events"` with:

- **4 partitions**  
- **16‑bit unsigned integer keys** (`u16` is the default)  
- **no registered message types** (so messages must be strings or bytes)

If you want to use dataclass messages, you can register them later.

---

## Producing Messages

Create a producer and write messages:

```python
producer = tmq.producer("events")

producer.write_message(12, "Hello world")

x = 12345
producer.write_message(42, x.to_bytes(16))
```

Message payloads may be:

- `str` → encoded as UTF‑8  
- `bytes` → stored as raw binary  
- dataclass instances → encoded as JSON (after registration)

Messages are appended atomically to the partition selected by the key.

---

## Consuming Messages

Consumers read messages and track their own offsets:

```python
consumer = tmq.consumer("events", group="event-readers")

print(consumer.poll())
consumer.commit()
print(consumer.poll())
```

### What happens here?

- The consumer joins the **event-readers** group (creating it if needed).
- A rebalance is triggered so the group can agree on partition ownership.
- After the rebalance window ends, partitions are assigned by consensus.
- `poll()` returns the next available message from the partitions this consumer owns.
- `commit()` records the consumer’s progress so it can resume later.

TwainMQ’s consumer‑group behaviour mirrors Kafka’s — but locally, using file‑based consensus rather than a broker.

---

## Consumer Groups

Multiple consumers in the same group share the work:

```python
c1 = tmq.consumer("events", group="workers")
c2 = tmq.consumer("events", group="workers")
```

TwainMQ will:

- detect both consumers  
- trigger a rebalance  
- assign partitions so each is processed by exactly one consumer  

If one consumer disappears, the remaining consumers will take over its partitions during the next rebalance.

---

## Using Dataclass Messages

Dataclass messages must be registered before use:

```python
from dataclasses import dataclass

@dataclass
class Event:
    id: int
    text: str

tmq.register_msg_cls(Event)

producer = tmq.producer("events")
producer.write_message(7, Event(id=1, text="hello"))
```

Consumers will automatically decode dataclass messages back into instances.

---

## Next Steps

Explore the rest of the documentation:

- **Tutorials** — real‑world examples and workflows  
- **Architecture** — how TwainMQ works internally  
- **API Reference** — full documentation for all classes and methods  

TwainMQ is small, readable, and designed to be understood. Feel free to explore the source code and adapt it to your needs.
