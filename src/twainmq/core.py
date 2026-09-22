from pathlib import Path
import logging
import json
import random
import shutil

from .consumer_groups import CONSUMER_GROUP_MESSAGE_CLASSES
from .consumer import TwainMQConsumer
from .producer import TwainMQProducer
from .encoding import _is_safe
from .errors import InvalidTopicNameError, InvalidKeyTypeError, TopicDeleteError

logger = logging.getLogger(__name__)

class Twain:
    """
    The central entry point for interacting with a TwainMQ installation.

    A `Twain` instance represents a complete TwainMQ environment rooted at a
    directory on disk. This directory contains all topics, configuration files,
    and global metadata. Applications typically create one long-lived `Twain`
    object and use it to manage topics, producers, consumers, and message
    registrations.

    Parameters
    ----------
    root_dir : str or Path
        Filesystem path to the TwainMQ root directory. This directory will hold
        all topic data, configuration, and state.

    Message Classes
    ---------------
    Dataclass-based messages must be registered with the `Twain` instance before
    they can be produced or consumed. Registration assigns each message type a
    stable numeric identifier used during encoding. All producers and consumers
    created from the same `Twain` share this registry.

    In rare cases where different topics require dataclasses with the same
    message name but different schemas, separate `Twain` instances must be used.
    (It is strongly recommended to avoid such naming collisions.)

    Notes
    -----
    - A `Twain` object is intended to be long-lived and reused across your
      application.
    - Global configuration (e.g. message type registry) is stored at the `Twain`
      level and applies to all producers and consumers created from it.
    - Topics created through this instance are immediately available for
      producing and consuming messages.

    Examples
    --------
    Create a Twain instance pointing at a local directory:

    >>> tmq = Twain("C:/TwainMQ")

    Create a new topic with 16-bit unsigned integer keys:

    >>> tmq.create_topic("hello_world", "u16")

    Produce a message:

    >>> producer = tmq.producer("hello_world")
    >>> producer.write_message(42, "Hello!")

    Consume messages:

    >>> consumer = tmq.consumer("hello_world")
    >>> msg = consumer.poll()
    """
    def __init__(self, root_dir):
        self._root_dir = Path(root_dir)
        self._msg_cls_registry = dict()
        for m in CONSUMER_GROUP_MESSAGE_CLASSES:
            self.register_msg_cls(m)

    def register_msg_cls(self, message_cls):
        """
        Register a dataclass message type for use with this Twain instance.

        Dataclass messages must be registered before they can be produced or
        consumed. Registration assigns the class a stable numeric identifier used
        during encoding and decoding.

        Parameters
        ----------
        message_cls : type
            A dataclass defining a message schema. The class name (or its
            `__message_type__` attribute, if present) is used as the message type
            identifier.

        Raises
        ------
        KeyError
            If a message class with the same name is already registered.
        """
        name = getattr(message_cls, "__message_type__", message_cls.__name__)
        if name in self._msg_cls_registry:
            raise KeyError(f"Class already registered: {name}")
        self._msg_cls_registry[name] = message_cls

    def create_topic(self, topic_name, key_type=None, partitions=1, message_types=None):
        """
        Create a new topic in this TwainMQ instance.

        Parameters
        ----------
        topic_name : str
            Name of the topic. Must contain only safe characters.
        key_type : str, optional
            Key encoding type. Defaults to `"u16"`. Supported values are `"u8"`,
            `"u16"`, `"u32"`, `"u64"`, or `"charN"` where `N` is the number of
            characters.
        partitions : int, optional
            Number of partitions for the topic. Defaults to 1.
        message_types : list of str, optional
            Names of dataclass message types used by this topic. These do not need
            to be registered yet; they are recorded in the topic configuration.

        Raises
        ------
        InvalidTopicNameError
            If the topic name contains invalid characters.
        InvalidKeyTypeError
            If `key_type` is not recognised.
        ValueError
            If the topic already exists.
        """
        key_types = dict(
        u8 = 1,
        u16 = 2,
        u32 = 4,
        u64 = 8,
        )

        if not _is_safe(topic_name):
            raise InvalidTopicNameError("Topic name contains invalid characters")

        if key_type is None:
            key_type = "u16"
        
        try:
            if key_type.startswith("char") and key_type[4:].isdigit():
                key_width = -int(key_type[4:])
            else:
                key_width = key_types[key_type]
        except KeyError:
            raise InvalidKeyTypeError(f"{key_type} is not a valid key_type. Options are {', '.join(key_types.keys())} or charN where N is an integer")
        
        topic_path = self._topic_path(topic_name)
        if topic_path.exists():
            raise ValueError(f"Cannot create topic, {topic_name} already exists")
        new_topic_dir = topic_path.mkdir()
        config_path = self._config_path(topic_name)
        if message_types is None:
            message_types = {}
        else:
            message_types = {m: i for i, m in enumerate(message_types)}
        config = dict(
            key_width = key_width,
            partitions = partitions,
            message_types = message_types,
        )
        
        with config_path.open("w", encoding="utf-8") as f:
            json.dump(config, f, indent = 0)

    def delete_topic(self, topic_name):
        """
        Delete a topic and all of its data.

        This operation is irreversible. A confirmation prompt is shown to prevent
        accidental deletion.

        Parameters
        ----------
        topic_name : str
            The name of the topic to delete.

        Returns
        -------
        None or TopicDeleteError
            Returns an error object if the user fails the confirmation prompt.
        """
        challenge_digit = random.randint(0, 9)
        confirm = input(f"To confirm delete of topic {topic_name} in {self.root_dir}, type YES{challenge_digit}")
        if confirm == f"YES{challenge_digit}":
            shutil.rmtree(self._topic_path(topic_name))
            logger.info(f"Topic deleted: {topic_name}")
        else:
            return TopicDeleteError("User confirm failed, topic not deleted")

    def producer(self, topic_name):
        """
        Create a producer for the given topic.

        Parameters
        ----------
        topic_name : str
            The topic to produce messages to.

        Returns
        -------
        TwainMQProducer
            A producer bound to the specified topic.
        """
        return TwainMQProducer(self, topic_name)

    def consumer(self, topic_name, start_from=None, group=None):
        """
        Create a consumer for the given topic.

        Parameters
        ----------
        topic_name : str
            The topic to consume from.
        start_from : {"start", "now"}, optional
            Initial read position when no committed offset exists. Defaults to
            `"start"`.
        group : str or None, optional
            Consumer-group identifier. If provided, the consumer joins the group and
            participates in consensus-based partition assignment and commits.

        Returns
        -------
        TwainMQConsumer
            A consumer bound to the specified topic.
        """
        if start_from is None:
            start_from = "start"
        return TwainMQConsumer(self, topic_name, start_from, group)

    def _topic_path(self, topic_name: str) -> Path:
        return self.root_dir / topic_name

    def topic_exists(self, topic_name):
        """
        Check whether a topic exists in this TwainMQ instance.

        Parameters
        ----------
        topic_name : str

        Returns
        -------
        bool
            True if the topic exists, False otherwise.
        """
        return self._topic_path(topic_name).exists()

    def list_topics(self):
        """
        List all topics in this TwainMQ instance.

        Returns
        -------
        list of str
            Names of all topics, excluding internal consumer‑group directories.
        """
        return [t.stem for t in self.root_dir.iterdir() if not t.name.startswith("--group--") if t.name.endswith(".twc")]

    def _config_path(self, topic_name):
        return self.root_dir / f"{topic_name}.twc"

    @property
    def root_dir(self):
        return self._root_dir
