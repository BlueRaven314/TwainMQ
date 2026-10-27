# Distributed Logging with TwainMQ

Python's built-in logging framework is excellent for writing log messages to files and consoles. Sometimes, however, it is useful to treat logs as a stream of structured events that can be consumed by other processes.  For example, if you wish to show log messages in a dashboard, or run automatic monitoring on them detect changes.

TwainMQ provides a TwainMQHandler class for use with the standard logging module which publishes log records directly to a TwainMQ topic. Each log record is converted into a LogEntry dataclass containing both the original logging metadata and the fully formatted log message.

This allows logs to be:

* Collected from multiple processes or machines
* Consumed by monitoring tools
* Stored and replayed through TwainMQ
* Processed as structured data rather than plain text

## Creating a Logging Topic

No manual setup is required.

When a TwainMQHandler is created it will:

* Create the topic if it does not already exist
* Register the LogEntry message type
* Create a producer for writing log records

```python
import logging
 
from twainmq import Twain
from twainmq.logger import TwainMQHandler
 
twain = Twain("./demo")
 
handler = TwainMQHandler(
    twain=twain,
    topic="logs",
)
 
logging.basicConfig(
    level=logging.INFO,
    handlers=[handler],
)
```

## Writing Log Messages

Once configured, any standard Python logger can be used.

```python
import logging
 
logger = logging.getLogger(__name__)
 
logger.info("Application started")
logger.warning("Configuration file not found")
logger.error("Database connection failed")
```

Log messages from all configured loggers will be published to the "logs" topic.

## Logging Exceptions

Exception information is automatically captured and included in the generated LogEntry.

```python
try:
    1 / 0
except ZeroDivisionError:
    logger.exception("Unexpected error")
```

The resulting LogEntry will contain:

* The logger name
* Severity level
* Source module and line number
* Formatted message
* Full exception traceback

## Reading Log Messages

Log records are received as LogEntry instances.

```python
from twainmq import Twain
from twainmq.logger import LogEntry
 
twain = Twain("./demo")
twain.register_msg_cls(LogEntry)

consumer = twain.consumer("logs")
 
while msg := consumer.poll():
    log = msg.message
 
    print(
        f"[{log.level}] "
        f"{log.logger}: "
        f"{log.message}"
    )
```

Example output:

```plain Text
[INFO] __main__: Application started
[WARNING] __main__: Configuration file not found
[ERROR] __main__: Database connection failed
```

## Accessing Structured Metadata

Because logs are transported as dataclasses rather than text, metadata can be accessed directly.

```python
while msg := consumer.poll():
    log = msg.message
    print(log.timestamp)
    print(log.logger)
    print(log.module)
    print(log.func_name)
    print(log.line_no)
```

This makes it easy to build monitoring, analytics, or alerting applications without needing to parse log files.

## Using the Formatted Message

The original formatted output produced by the configured logging formatter is preserved.

```python
formatter = logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s"
)
 
handler.setFormatter(formatter)
```

Consumers can access the resulting string through:

```python
log.formatted
```

For example:

```plain Text
2026-10-27 12:34:56 INFO Application started
```

## Logger Names and Partitioning

Messages are keyed using the logger name.

For example:

```python
logging.getLogger("database")
logging.getLogger("webserver")
logging.getLogger("pricing")
```

will produce keys based on:

```plain Text
database
webserver
pricing
```

If a logger name exceeds the maximum key width supported by the topic (default: 32 characters), the key is truncated for transport purposes. The complete logger name is always preserved in:

```python
log.logger
```

so no information is lost.

## Complete Example

Producer:

```python
import logging
 
from twainmq import Twain
from twainmq.logger import TwainMQHandler
 
twain = Twain("./demo")
 
handler = TwainMQHandler(
    twain=twain,
    topic="logs",
)
 
logging.basicConfig(
    level=logging.INFO,
    handlers=[handler],
)
 
logger = logging.getLogger("example")
 
logger.info("Hello TwainMQ")
```

Consumer:

```python
from twainmq import Twain
from twainmq.logger import LogEntry
 
twain = Twain("./demo")
twain.register_msg_cls(LogEntry)

consumer = twain.consumer("logs")
 
while msg := consumer.poll():
    log = msg.message
 
print(
    f"{log.level}: "
    f"{log.logger}: "
    f"{log.message}"
)
```

Output:

```plain text
INFO: example: Hello TwainMQ
```

This pattern turns Python logging into a distributed stream of structured events while remaining fully compatible with the standard logging module.