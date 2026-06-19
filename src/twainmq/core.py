from datetime import datetime, timezone
from abc import ABC
from pathlib import Path
from collections import namedtuple
import logging
import json
import random
import shutil

from twainmq.consumer import TwainMQConsumer

from .producer import TwainMQProducer
from .consumer_groups import CONSUMER_GROUP_MESSAGE_CLASSES
from .encoding import _is_safe, base85_to_key, find_key_char_width, key_to_base85
from .errors import InvalidTopicNameError, InvalidKeyTypeError, TopicDeleteError, ConfigNotFoundError
from .atomic_append import atomic_append
from dataclasses_jsonschema import JsonSchemaMixin

logger = logging.getLogger(__name__)

MessageTuple = namedtuple("MessageTuple", ["offset", "key", "timestamp", "message"])
MAX_MESSAGE_SIZE = 4096

class Twain:
    """
    The central entry point for interacting with a TwainMQ installation.

    A `Twain` instance represents a single TwainMQ environment rooted at a
    directory on disk. This directory holds all topics, configuration files,
    and global state. Typically, you create one `Twain` object per process
    and reuse it to manage topics, producers, and consumers.

    Parameters
    ----------
    root_dir : str or Path
        Filesystem path to the TwainMQ root directory. This directory will
        contain topic data, configuration, and metadata.

    Notes
    -----
    - The `Twain` object is designed to be long-lived. Create it once and
      share it across your application rather than instantiating multiple
      times.
    - Global configuration parameters (e.g. encoding defaults, safety
      thresholds) can be set at the `Twain` level and will apply to all
      producers and consumers created from it.
    - Message dataclasses are registered with the global twain, and then all consumers
      have access to these registrations.  In the unusual case where you have conflicting 
      messages with the same name on different topics, then you will need separate `Twain`
      instances (although I advise for your general sanity to try avoid doing this to your topics).

    Examples
    --------
    Create a Twain instance pointing at a local directory:

    >>> tmq = Twain("C:/TwainMQ")

    Create a new topic with 16-bit unsigned integer keys:

    >>> tmq.create_topic("hello_world", "u16")

    Create a producer for that topic and write a message:

    >>> producer = tmq.producer("hello_world")
    >>> producer.write_message(42)

    Create a consumer to read messages:

    >>> consumer = tmq.consumer("hello_world")
    >>> msg = consumer.read_message()
    """
    def __init__(self, root_dir):
        self._root_dir = Path(root_dir)
        self._msg_cls_registry = dict()
        for m in CONSUMER_GROUP_MESSAGE_CLASSES:
            self.register_msg_cls(m)

    def register_msg_cls(self, message_cls):
        name = getattr(message_cls, "__message_type__", message_cls.__name__)
        if name in self._msg_cls_registry:
            raise KeyError(f"Class already registered: {name}")
        self._msg_cls_registry[name] = message_cls

    def create_topic(self, topic_name, key_type=None, partitions=1, message_types=None):
        """Create a new topic
        
        Args:
            topic_name: The name of the topic
            key_type:  The key type ("u8", "u16", "u32", "u64", "char1", "char2", "char4", "char8", "char16"), default  = "u16"
            partitions: The number of partitions to split it into, default = 1          
            message_types: List of message dataclass names, these do not need to be registered at the time create_topic is called.
        """
        
        key_types = dict(
        u8 = 1,
        u16 = 2,
        u32 = 4,
        u64 = 8,
        char1 = -1,
        char2 = -2,
        char4 = -4,
        char8 = -8,
        char16 = -16,                        
        )

        if not _is_safe(topic_name):
            raise InvalidTopicNameError("Topic name contains invalid characters")

        if key_type is None:
            key_type = "u16"
        
        try:
            key_width = key_types[key_type]
        except KeyError:
            raise InvalidKeyTypeError(f"{key_type} is not a valid key_type. Options are {', '.join(key_types.keys())}")
        
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
        challenge_digit = random.randint(0, 9)
        confirm = input(f"To confirm delete of topic {topic_name} in {self.root_dir}, type YES{challenge_digit}")
        if confirm == f"YES{challenge_digit}":
            shutil.rmtree(self._topic_path(topic_name))
            logger.info(f"Topic deleted: {topic_name}")
        else:
            return TopicDeleteError("User confirm failed, topic not deleted")

    def producer(self, topic_name):
        return TwainMQProducer(self, topic_name)

    def consumer(self, topic_name, start_from=None, group=None):
        if start_from is None:
            start_from = "start"
        return TwainMQConsumer(self, topic_name, start_from, group)

    def _topic_path(self, topic_name: str) -> Path:
        return self.root_dir / topic_name

    def topic_exists(self, topic_name):
        """Checks if a topic exists"""
        return self._topic_path(topic_name).exists()

    def list_topics(self):
        """Returns a list of all the topics in this TwainMQ instance"""
        return [t.stem for t in self.root_dir.iterdir() if not t.name.startswith("--group--") if t.name.endswith(".twc")]

    def _config_path(self, topic_name):
        return self.root_dir / f"{topic_name}.twc"

    @property
    def root_dir(self):
        return self._root_dir

class TwainMQBase(ABC):
    def __init__(self, twain: Twain, topic: str):
        self._topic = topic
        self._twain = twain
        config_path = self._twain._config_path(topic)
        if not config_path.is_file():
            raise ConfigNotFoundError(f"No config for {topic}: cannot find file {config_path}")
        with config_path.open("r") as f:
            config = json.load(f)
        self._key_width = config["key_width"]
        self._message_types = config["message_types"]
        self._message_types_rev = {i: msg_type for msg_type, i in self._message_types.items()}
        self._n_partitions = int(config["partitions"])
        self._key_chars = find_key_char_width(self._key_width)
    
    @property
    def topic(self):
        return self._topic
    
    @property
    def _topic_dir(self):
        return self._twain.root_dir / self._topic
    
    @property
    def key_width(self):
        """The width of the key in bytes.  0 indicates a string key.
        """
        return self._key_width

    @property
    def chunk_str_now(self):
        return f"{datetime.now(timezone.utc):%Y%m%d}"
