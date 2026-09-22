# Architecture

TwainMQ is a lightweight, broker‑free message log built entirely on top of the filesystem.  
This section explains the internal design: how data is stored, how partitions work, how
consumer groups coordinate, and how messages move through the system.

TwainMQ’s architecture is intentionally simple. Everything is a file, every operation is
atomic, and all coordination happens through append‑only logs.

---

## Core Concepts

TwainMQ is built around a few core ideas:

- **Append‑only logs** as the fundamental storage primitive  
- **Partitions** for parallelism and ordering guarantees  
- **Consensus‑based consumer groups** without a central broker  
- **Atomic file operations** for durability  
- **Typed messages** encoded compactly and consistently  

These concepts mirror the design of distributed log systems like Kafka, but implemented
locally and without background services.

---

## Storage Layout

Each topic is stored as a directory inside the TwainMQ root along with a matching .twc file that defines the topic.  A topic directory contains the log files, one for each partition per day.

Partition files are append‑only. TwainMQ never rewrites or truncates them.

---

## Message Format

Every message written to a partition is encoded into a compact, self‑contained blob:

- fixed‑width key (integer or fixed‑width string)
- timestamp
- encoded payload (string, bytes, or dataclass)
- newline terminator

The payload is compressed and base85‑encoded. A magic byte at the start of the payload indicates the payload type if it isn't utf-8.
Dataclass messages include a small additional type identifier so consumers can decode them automatically.

Messages are appended using an atomic write operation to ensure durability even if the process crashes mid‑write.  The write logic is adapted
for each supported file system to ensure this behaviour.

---

## Partitioning Model

TwainMQ uses **key‑based partitioning**. The partitioner maps:

    (key, number_of_partitions) → partition_index

By default, a stable 64‑bit hash is used. Keys may be:

- integers  
- fixed‑width strings (`charN` topics)

The standard partitioner provides these guarantees:

- messages with the same key always go to the same partition  
- ordering is preserved within each partition  
- partitions can be processed independently and in parallel  

---

## Consumers and Offset Tracking

A consumer reads messages from the partitions it owns. Each consumer maintains:

- a list of assigned partitions
- an offset for each partition
- a record of the last successfully polled partition (for round‑robin fairness)

Offsets are stored locally in memory and committed to the consumer group when using
group mode.

Polling is round‑robin:

- the consumer rotates through its partitions  
- each call returns the next available message  
- no partition can starve another  

This behaviour differs from Kafka, where a single partition may dominate consumption if
it has a backlog.

---

## Consumer Groups and Coordination

TwainMQ implements consumer groups without a broker. Coordination happens through a
special **group log** stored in:

    --group--<group-name>/

Consumers periodically send heartbeat messages and rebalance requests into this log.

A rebalance works like this:

1. A consumer triggers a rebalance (cheap and safe).  
2. All consumers write a “still alive” message.
3. After the rebalance window ends, the group agrees on which consumers are active.
4. Partitions are assigned so each is owned by exactly one consumer.
5. Consumers update their local state and continue polling.

Rebalances never pause consumption. Paritions are assigned with priority to the existing owner
to minimise the number of "moves" that are made.  If all consumers respond, the rebalance is a no‑op.

This model provides Kafka‑like semantics without a central coordinator.

---

## Commits and Resume

Consumers in a group can commit their offsets. A commit records:

- the partition
- the offset
- the consumer ID
- a timestamp

When a consumer joins a group, it resumes from the last committed offset for each
partition it is assigned.

Standalone consumers (not in a group) cannot commit, but they can still read from the
beginning or from “now”.

---

## Message Lifecycle

A message moves through TwainMQ in five steps:

1. **Producer writes**
   The message is encoded, partitioned, and appended atomically.

2. **Stored on disk**  
   The message sits in an append‑only partition file.

3. **Consumer polls** 
   The consumer reads the next message from its assigned partitions.

4. **Consumer processes**
   The application handles the message payload.

5. **Commit (optional)** 
   If in a group, the consumer records its progress so others can resume.

This lifecycle is intentionally minimal. TwainMQ does not delete, compact, or reorder
messages. What you write is what you read.

(Compaction and deletion/archive is a planned features for the future)

---

## Summary

TwainMQ’s architecture is built on:

- simple, durable file operations  
- predictable partitioning  
- lightweight consumer‑group consensus  
- minimal moving parts  

The result is a message log that is easy to understand, easy to debug, and easy to embed
in small systems — while still offering many of the semantics of larger distributed
brokers.

Explore the rest of the architecture section for deeper details on:

- **Message Keys**  
- **Consumer Group Internals**  
- **Partitioning Strategies**  
- **Encoding and Message Types**
