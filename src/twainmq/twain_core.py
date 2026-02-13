from datetime import datetime
from abc import ABC, abstractmethod
from pathlib import Path
import string
import base64
import zlib
import numpy as np
import os
from collections import namedtuple
import unittest
import random
import shutil
import logging
import json
logger = logging.getLogger(__name__)

class TwainMQError(Exception): pass
class TopicCorruptError(TwainMQError): pass
class ConfigNotFoundError(FileNotFoundError, TwainMQError): pass
class TopicAlreadyExists(TwainMQError): pass
class NoActiveMessageFileToReadError(TwainMQError): pass
class InvalidKeyTypeError(TwainMQError): pass
class TopicDeleteError(TwainMQError): pass

MessageTuple = namedtuple("MessageTuple", ["offset", "key", "timestamp", "message"])

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
        
    def create_topic(self, topic_name, key_type = None, partitions = 1):
        """Create a new topic
        
        Args:
            twain_directory: The root directory for twain MQs
            topic_name: The name of the topic
            partitions: The number of partitions to split it into, default = 1
            key_type:  The key type ("u8", "u16", "u32", "u64", "str"), default  = "u16"
        """
        
        key_types = dict(
        u8 = 1,
        u16 = 2,
        u32 = 4,
        u64 = 8,
        str = 0,
        )
        
        if key_type is None:
            key_type = "u16"
        
        if key_type == "str":
            raise NotImplementedError("Support for string keys is not implemented yet")
        
        try:
            key_width = key_types[key_type]
        except KeyError:
            raise InvalidKeyTypeError(f"{key_type} is not a valid key_type. Options are {', '.join(key_types.keys())}")
           
        topic_path = self._topic_path(topic_name)
        if topic_path.exists():
            raise ValueError(f"Cannot create topic, {topic_name} already exists")
        new_topic_dir = topic_path.mkdir()
        config_path = self._config_path(topic_name)
        
        config = dict(key_width = key_width, partitions = partitions)
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

    def consumer(self, topic_name, group = None):
        return TwainMQConsumer(self, topic_name, group)

    def _topic_path(self, topic_name):
        return self.root_dir / topic_name

    def _config_path(self, topic_name):
        return self.root_dir / f"{topic_name}.twc"

    @property
    def root_dir(self):
        return self._root_dir

class TwainMQBase(ABC):
    def __init__(self, twain, topic):
        self.topic = topic
        self._twain = twain
        config_path = self._twain._config_path(topic)
        if not config_path.is_file():
            raise ConfigNotFoundError(f"No config for {topic}: cannot find file {config_path}")
        with config_path.open("r") as f:
            config = json.load(f)
        self._key_width = config["key_width"]
        self._partitions = int(config["partitions"])
        self._key_chars = ENCODED_WIDTHS[self._key_width]
        
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self.close()

    # @property
    # def _message_files_by_offset(self):
        # topic_dir = self._twain.root_dir / self.topic
        # message_files_by_offset = [(int(x.stem.split("_")[1]), x) for x in topic_dir.iterdir()]
        # return message_files_by_offset
    
    def __str__(self):
        return f"{self.__class__.__name__}(topic={self.topic}"
    
    @property
    def key_width(self):
        """The width of the key in bytes.  0 indicates a string key.
        """
        return self._key_width

    # @property
    # def offset(self):
        # return self._current_offset

    # def close(self):
        # if self._current_file_handle is not None:
            # self._current_file_handle.close()
        # self._current_file_handle = None

class TwainMQConsumer():
    """A consumer"""
    def __init__(self, twain, topic, group = None):
        self._twain = twain
        self._topic = topic
        self._group = group

class TwainMQConsumerlet(TwainMQBase):
    """A consumerlet is a simple single partition consumer.  Usually you would not use a Consumerlet directly, rather use the Consumer container"""
    def __init__(self, twain, partition, offset=None):
        super().__init__(twain, partition)

        self._current_file_handle = None

        if offset is None:
            self._current_offset = int(self._active_file.stem.split("_")[1])
            self._set_starting_file_from_offset(self._current_offset)
        elif offset == -1:
            self._current_offset = int(self._active_file.stem.split("_")[1])
            self._set_starting_file_from_offset(self._current_offset)
            self.read_all_messages()
        elif offset < -1:
            raise NotImplementedError("Cannot do specific offsets back from end yet")
        else:
            self._set_starting_file_from_offset(offset)
            self._current_offset = offset

        self.next_file_start = None
        for o, f in self._message_files_by_offset:
            if o > self._current_offset:
                self.next_file_start = o
                break

    def _set_starting_file_from_offset(self, offset):
        """
        Decide which file we should be reading given a logical offset.
        This preserves your original logic for explicit offsets.
        """
        reading_file_path = None
        for o, f in self._message_files_by_offset:
            if o > offset:
                # we've gone past the desired offset; last file we saw is correct
                break
            reading_file_path = f

        if reading_file_path is None:
            # no historical file; fall back to active file
            reading_file_path = self._active_file

        self._reading_file_path = reading_file_path
        self._current_file_handle = None  # lazy-open

    def _ensure_handle(self):
        """
        Make sure we have an open handle for the current reading file.
        Returns True if handle is ready, False if file does not exist yet.
        """
        if self._current_file_handle is not None:
            return True

        if self._reading_file_path.exists():
            self._current_file_handle = self._reading_file_path.open("r")
            return True

        return False

    def _rollover_to_next_file_if_needed(self):
        """
        Handle rollover to the next chunk file based on next_file_start.
        Returns True if we rolled over, False otherwise.
        """
        if self.next_file_start is None:
            return False

        if self._current_offset < self.next_file_start:
            return False

        # close current and move to next
        if self._current_file_handle is not None:
            self._current_file_handle.close()
            self._current_file_handle = None

        reading_file_path = dict(self._message_files_by_offset)[self.next_file_start]
        self._reading_file_path = reading_file_path
        self._current_offset = self.next_file_start

        # recompute next_file_start
        self.next_file_start = None
        for o, f in self._message_files_by_offset:
            if o > self._current_offset:
                self.next_file_start = o
                break

        return True

    def _rollover_to_new_day_if_needed(self):
        """
        Handle date-based rollover: when the UTC date changes and there is a new active file.
        Mirrors your original chunk_str logic.
        """
        chunk_str_now = f"{datetime.utcnow():%Y%m%d}"
        if self.next_file_start is not None:
            return  # we already know about a next file by offset; let that logic win

        if chunk_str_now > self._chunk_str:
            # we've moved to a new day; switch to the new active file
            if self._current_file_handle is not None:
                self._current_file_handle.close()
                self._current_file_handle = None

            self._reading_file_path = self._active_file
            self._current_offset = int(self._active_file.stem.split("_")[1])
            self._chunk_str = chunk_str_now  # keep internal date in sync

    def _decode_line(self, offset, line):
        key = base85_to_int(line[:self._key_chars], self.key_width)
        timestamp = decode_datetime(line[self._key_chars:10 + self._key_chars])
        message = decode_message(line[10 + self._key_chars:])
        return MessageTuple(
            offset=offset,
            key=key,
            timestamp=timestamp,
            message=message,
        )
        
    def poll(self):
        """
        Return the next MessageTuple, or None if no message is currently available.
        """
        if not self._ensure_handle():
            return None

        line = self._current_file_handle.readline()
        if not line:
            if self._rollover_to_next_file_if_needed():
                return self.poll()

            self._rollover_to_new_day_if_needed()

            # after rollover attempts, try again once
            if self._rollover_to_next_file_if_needed() and self._ensure_handle():
                line = self._current_file_handle.readline()
                if not line:
                    return None
            else:
                return None

        # decode and advance offset
        msg = self._decode_line(self._current_offset, line.rstrip("\n"))
        self._current_offset += 1
        return msg

    def read_all_messages(self):
        """
        Drain all currently available messages starting from the consumer's current position.
        """
        messages = []
        while True:
            msg = self.poll()
            if msg is None:
                break
            messages.append(msg)
        return messages

    # --- legacy hook kept for compatibility ------------------------------------

    def _set_current_file_handle(self):
        """
        Kept for compatibility with existing code paths that might call this.
        Now just delegates to the new lazy-open logic.
        """
        if not self._ensure_handle():
            raise NoActiveMessageFileToReadError(
                f"Expected broker file {self._reading_file_path} does not exist yet"
            )

    def _initNewMessageFile(self, active_file, offset):
        print("Not initialising - read only")
        pass

class TwainMQProducer(TwainMQBase):
    def __init__(self, twain, topic, partitioner = None):
        super().__init__(twain, topic)
        if partitioner is None:
            partitioner = partition_hash64
        self._partitioner = partitioner
        self._active_files, self._chunk_str = self._get_active_message_files()
    
    def write_message(self, key, message):
        encoded_key = int_to_base85(key, self.key_width)
        partition = self._partitioner(key, self._partitions)
        timestamp = encode_datetime(datetime.utcnow())
        msg_blob = encode_message(message)
        binary_msg = f"{encoded_key}{timestamp}{msg_blob}\n".encode("utf-8")
        with self._active_file(partition).open("ab", buffering=0) as f:
            f.write(binary_msg)

    def _get_active_message_files(self):
        """Searches for the current active message files across all partitions"""
        topic_dir = self._twain.root_dir / self.topic
        active_files = dict()
        message_files = list(topic_dir.iterdir())        
        
        for partition in range(self._partitions):   
            chunk_str = f"{datetime.utcnow():%Y%m%d}"
            chunk_part_str = f"{partition}-{chunk_str}"
            active_file = [x for x in message_files if x.stem.split("_")[0] == chunk_part_str]
            if len(active_file) == 0:
                partition_files = [x for x in message_files if x.stem.split("-")[0] == partition]
                if len(partition_files) == 0:
                    file_offset = 0
                else:
                    prev_file = max(partition_files)
                    with prev_file.open("r") as f:
                        prev_file_len = len(f.readlines())
                    prev_file_offset = int(prev_file.stem.split("_")[1])
                    file_offset = prev_file_offset + prev_file_len
                new_active_file = topic_dir / f"{chunk_part_str}_{file_offset}.tmf"
                self._init_new_message_file(new_active_file)
                active_files[partition] = new_active_file
            elif len(active_file) == 1:
                active_files[partition] = active_file[0]
            else:
                raise TopicCorruptError(f"Multiple message files for the same chunk partition {partition}-{chunk_str}")
        return active_files, chunk_str
    
    def _init_new_message_file(self, new_active_file):
        new_active_file.touch()
    
    def _active_file(self, partition):
        """Returns the current file to be written to for a partition"""
        chunk_str = f"{datetime.utcnow():%Y%m%d}"
        if chunk_str != self._chunk_str:
            self._active_files, self._chunk_str = self._get_active_message_files()
        return self._active_files[partition]

    def close(self):
        pass

_RAW_MESSAGE = b"\x98" # unused so far
_GZIP_BYTES = b"\x99"
## Anything in the range \x80 to \xBF ought to be safe to use as sentinal bytes

def encode_datetime(dt):
    """Return 10 byte encoded date string"""
    return base64.b85encode(np.array(dt.timestamp()).tobytes()).decode("utf-8")
    
def decode_datetime(dt):
    return datetime.fromtimestamp(np.frombuffer(base64.b85decode(dt.encode("utf-8")))[0])

def encode_message(message):
    if isinstance(message, bytes):
        payload = _GZIP_BYTES + message
    else:
        payload = message.encode("utf-8")
    # Need to test to optimise the compression rate
    compressed = zlib.compress(payload, level=6, wbits=-15)
    return base64.b85encode(compressed).decode("utf-8")

def decode_message(message):
    compressed = base64.b85decode(message.encode("utf-8"))
    decoded = zlib.decompress(compressed, wbits=-15)
    if decoded.startswith(_GZIP_BYTES):
        return decoded[1:]
    else:
        return decoded.decode("utf-8")

def int_to_base85(n: int, width: int) -> str:
    """
    Encode an unsigned integer into a fixed-length Base85 string.

    Args:
        n: The unsigned integer to encode.
        width: Number of bytes to represent the integer (default 8 = 64-bit).

    Returns:
        A fixed-length Base85 string.
    """
    b = n.to_bytes(width, byteorder="big", signed=False)
    encoded = base64.b85encode(b)
    return encoded.decode("ascii")

ENCODED_WIDTHS = {i: len(int_to_base85(1, width = i)) for i in range(1, 8)}

def base85_to_int(s: str, width: int) -> int:
    """
    Decode a Base85 string back into an unsigned integer.
    """
    b = base64.b85decode(s.encode("ascii"))
    return int.from_bytes(b, byteorder="big", signed=False)

def partition_hash64(x, partitions) -> int:
    """Using splitmix64 to convert the integer key into a partition number for even mixing."""
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
    x = (x ^ (x >> 27)) * 0x94d049bb133111eb
    x = x ^ (x >> 31)
    return x % partitions

class TestBase85Encoding(unittest.TestCase):
    def test_round_trip_small_numbers(self):
        for n in [0, 1, 42, 255, 256, 12345]:
            enc = int_to_base85(n, width=2)
            dec = base85_to_int(enc, width=2)
            self.assertEqual(dec, n)

    def test_round_trip_large_numbers(self):
        # Max 64-bit unsigned integer
        n = 2**64 - 1
        enc = int_to_base85(n, width=8)
        dec = base85_to_int(enc, width=8)
        self.assertEqual(dec, n)

    def test_fixed_length_output(self):
        n = 123
        enc = int_to_base85(n, width=1)
        self.assertEqual(len(enc), 2)
        n = 123456789
        enc = int_to_base85(n, width=4)
        self.assertEqual(len(enc), 5)
        enc = int_to_base85(n, width=8)
        self.assertEqual(len(enc), 10)

    def test_different_numbers_produce_different_encodings(self):
        enc1 = int_to_base85(123, width=1)
        enc2 = int_to_base85(124, width=1)
        self.assertNotEqual(enc1, enc2)

if __name__ == "__main__":
    unittest.main()
