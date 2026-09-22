# Message Keys

Message keys determine **which partition** a message is written to.  
They also provide ordering guarantees: all messages with the same key always go to the same partition.

TwainMQ supports two key families:

- **Integer keys** (`u8`, `u16`, `u32`, `u64`)  
- **Fixed‑width string keys** (`charN`, where `N` is the number of characters)

This page explains how keys are interpreted, encoded, and used for partitioning.

---

## Why Keys Matter

Keys serve two purposes:

1. **Partition selection**  
   The partitioner maps `(key, number_of_partitions)` → `partition_index`.

2. **Ordering guarantees**  
   Messages with the same key are always appended to the same partition, preserving order.

Keys do *not* affect message payloads or consumer behaviour beyond partitioning.

---

## Integer Keys

Integer keys are the simplest and fastest option. TwainMQ supports:

- `u8`  — 1‑byte unsigned integer  
- `u16` — 2‑byte unsigned integer  
- `u32` — 4‑byte unsigned integer  
- `u64` — 8‑byte unsigned integer  

These types are specified when creating a topic:

    tmq.create_topic("events", key_type="u16")

The integer is encoded into a fixed‑width base85 string so that:

- all keys have the same encoded length  
- partition files remain easy to parse  
- sorting and debugging are straightforward

---

## Fixed‑Width String Keys (`charN`)

TwainMQ also supports fixed‑width string keys:

- `char1`  
- `char2`  
- `char3`  
- …  
- `charN` (any positive integer)

Example:

    tmq.create_topic("users", key_type="char8")

This means:

- every key must be a string of exactly 8 characters  
- shorter strings will be padded by the TwainMQ  
- longer strings are invalid

Fixed‑width strings are encoded directly into base85 with a negative `key_width` internally, but users never see this detail.

This mode is ideal for:

- short IDs  
- fixed‑format identifiers  
- embedded systems with strict key layouts  

---

## How Keys Are Encoded

Regardless of type, keys are encoded using:

- **base85** (compact, printable, filesystem‑safe)  
- **fixed width** (determined by the topic’s `key_type`)  

This ensures:

- consistent message formatting  
- predictable offsets  
- easy debugging with plain text tools  
- encoded key contains no special characters

---

## Partitioning

TwainMQ uses a partitioner function:

    partition = partitioner(key, number_of_partitions)

By default, this is a stable 64‑bit hash:

- fast  
- deterministic  
- evenly distributed  

You may supply your own partitioner when creating a producer:

    producer = tmq.producer("events", partitioner=my_custom_partitioner)

A partitioner function must return an integer from range(number_of_partitions).

The default partitioner guarantees:

- same key → same partition  
- different keys → likely different partitions  
- ordering preserved within each partition  

---

## Key Validation

When producing messages:

- integer keys must fit the declared type  
- string keys must fit the declared width  
- invalid keys raise an error immediately  

This prevents malformed data from entering the log.

---

## Choosing a Key Type

For performance you should use the smallest key that fits your use case, however once a key width is chosen it is permanent, so allow space for future expansion.

In most situations integer keys will have a slight performance edge over string keys, but that is likely outweighed by any integer to string mapping that you might later do.  Very long strings will obviously impact performance.

---

## Summary

TwainMQ keys are:

- **simple**
- **fixed‑width**
- **base85‑encoded**  
- **used only for partitioning and ordering**  

They ensure that messages are distributed predictably across partitions and that consumers can process them efficiently.

For more details, see:

- **Architecture → Partitioning**  
- **Producer API → write_message**  
- **Topic Creation → key_type**
