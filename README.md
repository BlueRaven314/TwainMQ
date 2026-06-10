# TwainMQ

**TwainMQ** is a lightweight, file‑based message log inspired by Kafka’s design principles but without the operational overhead of running a broker or cluster. It provides durable, append‑only topics, partitioned message streams, consumer groups with load balancing, and offset managemen, all without using a central service or broker.

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

## Documentation

Full documentation is available at:

**https://blueraven314.github.io/TwainMQ/**

## Why “TwainMQ”?  

Kafka messaging is powerful, but it’s also famously complex and sometimes hard going, rather like reading Franz Kafka. Mark Twain, on the other hand, is known for clarity and accessibility. TwainMQ takes inspiration from Kafka’s log‑based design, but aims to be simple, easy to use, and friendly to work with.