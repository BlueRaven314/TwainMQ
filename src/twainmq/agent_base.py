from abc import ABC
from datetime import datetime, timezone
import json

from twainmq.encoding import find_key_char_width
from twainmq.errors import ConfigNotFoundError

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .core import Twain

class TwainMQBase(ABC):
    def __init__(self, twain: "Twain", topic: str):
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
