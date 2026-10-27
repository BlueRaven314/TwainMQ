import logging
import traceback
from typing import TYPE_CHECKING
from dataclasses import dataclass
from logging import LogRecord

if TYPE_CHECKING:
    from .core import Twain

@dataclass(slots=True, frozen=True)
class LogEntry:
    """
    Structured representation of a logging.LogRecord.

    Instances are published by TwainMQHandler and contain both the
    original logging metadata and the fully formatted log message.

    The 'logger' field always contains the full logger name even if
    the transport key was truncated to satisfy topic key constraints.
    """
    logger: str
    level: str
    level_no: int
    timestamp: float

    module: str
    func_name: str
    line_no: int

    message: str
    formatted: str

    thread_name: str | None = None
    process_name: str | None = None
    exception: str | None = None

    @classmethod
    def from_record(
        cls,
        record: LogRecord,
        formatted: str,
    ) -> "LogEntry":
        """
        Create a LogEntry from a standard LogRecord.

        Parameters
        ----------
        record:
            The LogRecord produced by the Python logging framework.

        formatted:
            The fully formatted log message produced by the configured
            formatter.

        Returns
        -------
        LogEntry
            A serialized representation of the log event suitable for
            publication via TwainMQ.
        """
        if record.exc_info:
            exception = "".join(
                traceback.format_exception(*record.exc_info)
            )
        else:
            exception = None

        return cls(
            logger=record.name,
            level=record.levelname,
            level_no=record.levelno,
            timestamp=record.created,

            module=record.module,
            func_name=record.funcName,
            line_no=record.lineno,

            message=record.getMessage(),
            formatted=formatted,

            thread_name=record.threadName,
            process_name=record.processName,
            exception=exception,
        )

class TwainMQHandler(logging.Handler):
    """
    Logging handler that publishes log records to a TwainMQ topic.

    Log records are converted into LogEntry instances and written to
    the specified topic. If the topic does not already exist it will
    be created automatically.

    Messages are keyed using the first 32 characters of the logger name.
    The complete logger name is preserved in the LogEntry payload.
    """
    def __init__(self, twain: "Twain", topic: str, level: int=logging.NOTSET,  partitions: int=4):
        """
        Create a TwainMQ-backed logging handler.

        Parameters
        ----------
        twain:
            Twain instance used for logging.

        topic:
            Topic to which log messages will be published.

        level:
            Minimum logging level handled by this handler.

        partitions:
            Number of partitions to create if the topic does not
            already exist.
        """
        super().__init__(level)
        if not twain.topic_exists(topic):
            twain.create_topic(topic, "char32", partitions=partitions, message_types=["LogEntry"])
        twain.register_msg_cls(LogEntry)
        self.producer = twain.producer(topic)
        self.topic = topic

    def emit(self, record: logging.LogRecord) -> None:
        """
        Publish a log record to TwainMQ.

        The record is converted to a LogEntry and written to the
        configured topic. Any exceptions raised during formatting or
        publishing are handled via the standard logging error handling
        mechanism.
        """
        try:
            entry = LogEntry.from_record(
                record,
                formatted=self.format(record),
            )

            self.producer.write_message(
                message=entry,
                key=entry.logger[:32],
            )

        except Exception:
            self.handleError(record)