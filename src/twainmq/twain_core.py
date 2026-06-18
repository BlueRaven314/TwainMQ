from datetime import datetime, timedelta, timezone
from abc import ABC, abstractmethod
from pathlib import Path
import string
import base64
import zlib
import struct
import os
from collections import namedtuple
import unittest
import bisect
import logging
import json
import random
import shutil
import re
import time
from dataclasses import fields, is_dataclass, asdict, dataclass

from .consumer_groups import CONSUMER_GROUP_MESSAGE_CLASSES, CONSUMER_GROUP_MESSAGE_SET, REBAL_LENGTH, AbortJoin, BeginRebal, Commit, EndRebal, Joined, RebalConfirm, RebalInProgress, RebalOffer, _group_topic_name
from .encoding import _DATACLASS_MAGIC, _GZIP_MAGIC, base85_to_key, dataclass_from_dict, decode_datetime, encode_datetime, find_key_char_width, key_to_base85, partition_hash64
from .errors import MessageTooLongError, TopicCorruptError, InvalidGroupNameError, InvalidTopicNameError, InvalidKeyTypeError, TopicDeleteError, ConfigNotFoundError
from .atomic_append import atomic_append
from dataclasses_jsonschema import JsonSchemaMixin

logger = logging.getLogger(__name__)

MessageTuple = namedtuple("MessageTuple", ["offset", "key", "timestamp", "message"])
MAX_MESSAGE_SIZE = 4096

VALID_NAME_RE = re.compile(r'^[A-Za-z0-9_.-]+$')

def _is_safe(name: str) -> bool:
    return bool(VALID_NAME_RE.fullmatch(name))

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

class TwainMQConsumer(TwainMQBase):
    """A consumer
    
    A consumer does not need to be part of a consumer group, but if it is not, then it will not be able to commit.

    If a consumer is part of a group (even a group of one) then it can commit to record where it got to and then a consumer (re-)joining that group will pick up
    from where it left off

    `start_from` is only used when there is no consumer group, or when no commit has been made in a group.
    """
    def __init__(self, twain, topic, start_from = "start", group = None):
        super().__init__(twain, topic)
        self._twain = twain
        self._topic = topic
        self._group = group
        self._rebal_ending = None
        self._consumer_id = None
        self._rebal_in_progress = None
        self._commit_record = dict()
        if group is None:
            self._partitions = [i for i in range(self._n_partitions)]
            if start_from == "start":
                self._consumerlets = {p: TwainMQConsumerlet(twain, topic, p, offset=0) for p in self._partitions}
            elif start_from == "now":
                self._consumerlets = {p: TwainMQConsumerlet(twain, topic, p, offset=None) for p in self._partitions}
            else:
                raise ValueError(f'start_from should be either "start" of "now", received: "{start_from}"')
        else:
            self._partitions = []      
            self._consumerlets = dict()      
            self._join_group(group, start_from)
        self._last_polled = 0
        self.heartbeat()

    def _create_consumer_group(self, group_topic, start_from):
        self._twain.create_topic(group_topic, key_type = "u32", partitions = 1, message_types = CONSUMER_GROUP_MESSAGE_SET)
        with self._twain.producer(group_topic) as gprod:
            if start_from == "start":
                init_commits = [(i, 0) for i in range(self._n_partitions)]
            elif start_from == "now":
                init_commits = [(i, -1) for i in range(self._n_partitions)]
            else:
                raise ValueError(f'start_from should be either "start" of "now", received: "{start_from}"')
            gprod.write_message(0, Commit(init_commits))

    def _join_group(self, group, start_from, retry=3):
        """Called when a consumer joins a group to handle id registration with the group"""
        group_topic = _group_topic_name(group)
        if not self._twain._topic_exists(group_topic):
            self._create_consumer_group(group_topic, start_from)
        gprod = self._twain.producer(group_topic)
        gcon = self._twain.consumer(group_topic, start_from = "start", group = None)
        self._group_producer = gprod
        self._group_consumer = gcon
        
        keys_taken = set()
        while (msg := gcon.poll()) is not None:
            keys_taken.add(msg.key)
            self._process_group_msg(msg)
            
        consumer_id = max(keys_taken) + 1
        join_key = random.getrandbits(64)
        gprod.write_message(consumer_id, Joined(join_key))
        
        while True:
            msg = gcon.poll()
            if msg is None:
                time.sleep(0.5)
            if isinstance(msg.message, Joined):
                key = msg.message.tag
                if join_key == key:
                    self._consumer_id = consumer_id
                    self._begin_rebal()
                    return
                else:
                    gprod.write_message(consumer_id, AbortJoin(key))
                    if retry > 0:
                        time.sleep(random.random() * 0.1)
                        return self._join_group(group, start_from, retry = retry - 1)
                    else:
                        return
            else:
                self._process_group_msg(msg)
    
    def trigger_rebal(self):
        """Triggers a rebalance in the consumer group.  This is cheap and safe and should be done periodically to confirm that all the consumers
        in the group are still operating as they should.
        
        If everybody responds the rebalance is a null op and will always leave the partition assignments unchanged.
        """
        self._begin_rebal()

    def _begin_rebal(self):
        gprod = self._group_producer
        rebal_end_ts = (datetime.now(timezone.utc) + REBAL_LENGTH).timestamp()
        gprod.write_message(self._consumer_id, BeginRebal(rebal_end_ts))
        self.heartbeat()
    
    def _rebal_participate(self):
        gprod = self._group_producer
        gprod.write_message(self._consumer_id, RebalOffer(self._partitions, [i for i in range(self._n_partitions)]))

    def _end_rebal(self):
        gprod = self._group_producer
        gprod.write_message(self._consumer_id, EndRebal())
        self._confirm_rebal()

    def _confirm_rebal(self):
        gprod = self._group_producer
        assert self._rebal_in_progress is not None
        assignements = self._rebal_in_progress.get_assignments()
        self._partitions = assignements.get(self._consumer_id, [])
        self._consumerlets = {p: self._consumerlets.get(p, TwainMQConsumerlet(self._twain, self._topic, p, offset=self._commit_record.get(p,0))) for p in self._partitions}
        gprod.write_message(self._consumer_id, RebalConfirm(sorted(list(self._partitions))))
        self._rebal_in_progress = None

    def commit(self, partitions = None):
        if self._group is not None:
            if partitions is None:
                partitions = self._partitions
            commit_list = [(p, self._consumerlets[p]._offset) for p in partitions if p in self._consumerlets]
            print(commit_list)
            gprod = self._group_producer
            gprod.write_message(self._consumer_id, Commit(commit_list))

    def _handle_commit_msg(self, commit_msg):
        for p,o in commit_msg.partition_offsets:
            self._commit_record[p] = o

    def _process_group_msg(self, msg):
        if isinstance(msg.message, BeginRebal):
            if self._consumer_id:
                self._rebal_in_progress = RebalInProgress(msg.message.timestamp)
                self._rebal_participate()
        elif isinstance(msg.message, RebalOffer):
            if self._rebal_in_progress:
                self._rebal_in_progress.add_offer(msg.key, msg.message)
        elif isinstance(msg.message, EndRebal):
            if self._rebal_in_progress:
                self._confirm_rebal()
        elif isinstance(msg.message, Commit):
            self._handle_commit_msg(msg.message)
        else:
            pass

    def heartbeat(self):
        """Must be called periodically if using a consumer group otherwise you might leave the consumer group.
        Calling `poll` will call heartbeat first"""
        if self._group is not None:
            while msg := self._group_consumer.poll():
                self._process_group_msg(msg)
            if self._rebal_in_progress and self._rebal_in_progress.rebal_ending < datetime.now(timezone.utc).timestamp():
                self._end_rebal()
        
    def poll(self):
        self.heartbeat()
        n = len(self._partitions)
        for p_offset in range(n):
            partition_to_poll = self._partitions[(self._last_polled + p_offset + 1) % n]
            this_consumerlet = self._consumerlets[partition_to_poll]
            msg = this_consumerlet.poll()
            if msg is not None:
                self._last_polled = partition_to_poll
                return msg
    
    def poll_many(self, n=10):
        """Simply calls the poll method n times and returns the results as a list."""
        msgs = []
        for i in range(n):
            msg = self.poll()
            if msg is None:
                break
            msgs.append(msg)
        return msgs

    def __str__(self):
        return f"TwainMQConsumer(topic={self._topic}, group={self._group})"

class TwainMQConsumerlet(TwainMQBase):
    """A consumerlet is a simple single partition consumer.  Usually you would not use a Consumerlet directly, rather use the Consumer container"""
    def __init__(self, twain, topic, partition, offset=None):
        super().__init__(twain, topic)
        self._partition = partition
        self._seek_active_file(offset)

    def decode_message(self, message):
        compressed = base64.b85decode(message.encode("utf-8"))
        decoded = zlib.decompress(compressed, wbits=-15)
        if decoded.startswith(_GZIP_MAGIC):
            return decoded[1:]
        elif decoded.startswith(_DATACLASS_MAGIC):
            class_id = int.from_bytes(decoded[1:3], "big", signed=False)
            message_type = self._twain._msg_cls_registry[self._message_types_rev[class_id]]
            data = json.loads(decoded[3:])
            return dataclass_from_dict(message_type, data)
        else:
            return decoded.decode("utf-8")
    
    def _seek_active_file(self, offset = None):
        """Sets the file handle to the active file to read from and seeks to the end.
        
        If offset is None this is read from latest
        
        If the latest file is missing then it will return None.
        """
        part_files = self._list_partition_files()
        if offset is None or len(part_files) == 0:
            chunk_str = self.chunk_str_now
            head_file = [f for f in part_files if f.stem.split("-")[1].split("_")[0] == chunk_str]
            if len(head_file) == 0:
                self._current_file_handle = None
            elif len(head_file) == 1:
                offset = int(head_file[0].stem.split("_")[1])
                self._current_file_handle = head_file[0].open("r", encoding = "utf-8")
                while self._current_file_handle.readline():
                    offset += 1
                self._offset = offset
                self._chunk_str = chunk_str
            else:
                raise TopicCorruptError(f"Multiple message files for the same chunk partition {self._partition}-{chunk_str}")
        elif offset >= 0:
            offsets_files_chunks = sorted([(int(f.stem.split("_")[1]), f, f.stem.split("_")[0].split("-")[1]) for f in part_files])
                        
            offsets = [o for o, _, _ in offsets_files_chunks]
            i = bisect.bisect_right(offsets, offset) - 1
            this_offset, current_file, chunk_str = offsets_files_chunks[max(i, 0)]

            self._current_file_handle = current_file.open("r", encoding = "utf-8")
            self._chunk_str = chunk_str
            self._offset = this_offset
            while self._offset < offset:
                line = self._current_file_handle.readline()
                if line != "":
                    self._offset += 1
                else:
                    break
        else:
            raise NotImplementedError("Seek back from end not yet implemented")
    
    def _list_partition_files(self):
        return [f for f in self._topic_dir.iterdir() if f.stem.split("-")[0] == str(self._partition)]

    def poll(self):
        """Polls for the next message without blocking.  Returns None if no new message available."""
        if self._current_file_handle is None:
            self._seek_active_file()
            if self._current_file_handle is None:
                return None
        msg_line = self._current_file_handle.readline()[:-1]
        if msg_line == "":
            if self.chunk_str_now == self._chunk_str:
                return None
            else:
                self._seek_active_file(self._offset)
                msg_line = self._current_file_handle.readline()[:-1]
                if msg_line == "":
                    return None
        key = base85_to_key(msg_line[:self._key_chars], self._key_width)
        timestamp = decode_datetime(msg_line[self._key_chars:self._key_chars+10])
        message = self.decode_message(msg_line[self._key_chars+10:])
        msg_tuple = MessageTuple(
            offset=self._offset,
            key=key,
            timestamp=timestamp,
            message=message
        )
        self._offset += 1
        return msg_tuple

    @property
    def partition(self):
        return self._partition

class TwainMQProducer(TwainMQBase):
    def __init__(self, twain, topic, partitioner = None, options = None):
        super().__init__(twain, topic)
        if partitioner is None:
            partitioner = partition_hash64
        self._partitioner = partitioner
        self._active_files, self._chunk_str = self._get_active_message_files()

    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self.close()

    def encode_message(self, message):
        if is_dataclass(message):
            assert not isinstance(message, type)
            json_str = json.dumps(asdict(message), separators=(",", ":"))
            cls_name = getattr(type(message), "__message_type__", type(message).__name__)
            cls_id = self._message_types[cls_name].to_bytes(2, "big", signed = False)
            payload = _DATACLASS_MAGIC + cls_id + json_str.encode("utf-8")
        elif isinstance(message, bytes):
            payload = _GZIP_MAGIC + message
        else:
            payload = message.encode("utf-8")
        compressed = zlib.compress(payload, level=6, wbits=-15)
        return base64.b85encode(compressed).decode("utf-8")
        
    def write_message(self, key, message):
        encoded_key = key_to_base85(key, self.key_width)
        partition = self._partitioner(key, self._n_partitions)
        timestamp = encode_datetime(datetime.now())
        msg_blob = self.encode_message(message)
        binary_msg = f"{encoded_key}{timestamp}{msg_blob}\n".encode("utf-8")
        if len(binary_msg) > MAX_MESSAGE_SIZE:
            raise MessageTooLongError("Message exceeds max message size: {len(binary_msg)} bytes > {MAX_MESSAGE_SIZE} bytes")
        atomic_append(self._active_file(partition), binary_msg)

    def _get_active_message_files(self):
        """Searches for the current active message files across all partitions"""
        topic_dir = self._topic_dir
        active_files = dict()
        message_files = list(topic_dir.iterdir())
        
        for partition in range(self._n_partitions):  
            chunk_str = self.chunk_str_now
            chunk_part_str = f"{partition}-{chunk_str}"
            active_file = [x for x in message_files if x.stem.split("_")[0] == chunk_part_str]
            if len(active_file) == 0:
                partition_files = [x for x in message_files if x.stem.split("-")[0] == str(partition)]
                if len(partition_files) == 0:
                    file_offset = 0
                else:
                    prev_file = max(partition_files)
                    with prev_file.open("r") as f:
                        lines = f.readlines()
                        prev_file_len = len(lines)
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
        if self.chunk_str_now != self._chunk_str:
            self._active_files, self._chunk_str = self._get_active_message_files()
        return self._active_files[partition]

    def close(self):
        pass

def multi_poll(consumers, offset=None):
    """Helper function to poll evenly from multiple consumers.  This is useful because with TwainMQ a consumer can only subscribe to one topic, 
    so when you need to consume from multiple topics you need multiple consumers.
    
    Provide a list of consumers, and an optional offset, and this will poll the consumers in turn until one returns a message.  It will return the index
    of the consumer that returned the message.  The offset passed in will always be the index of last consumer to be called.  If you store the index from the previous
    call and pass it back in as the offset you will get a round robin calling sequence. 
    
    If offset is not specified (or set to None) then a random offset is chosen, which means that topics will be polled in a random order each time it is called."""
    n = len(consumers)
    if offset is None:
        offset = random.randrange(n)    
    for c_offset in range(n):
        consumer_to_poll = consumers[(c_offset + offset + 1) % n]
        msg = consumer_to_poll.poll()
        if msg is not None:
            return ((c_offset + offset + 1) % n, msg)

class TestBase85Encoding(unittest.TestCase):
    def test_round_trip_small_numbers(self):
        for n in [0, 1, 42, 255, 256, 12345]:
            enc = key_to_base85(n, width=2)
            dec = base85_to_key(enc, width=2)
            self.assertEqual(dec, n)

    def test_round_trip_large_numbers(self):
        # Max 64-bit unsigned integer
        n = 2**64 - 1
        enc = key_to_base85(n, width=8)
        dec = base85_to_key(enc, width=8)
        self.assertEqual(dec, n)

    def test_fixed_length_output(self):
        n = 123
        enc = key_to_base85(n, width=1)
        self.assertEqual(len(enc), 2)
        n = 123456789
        enc = key_to_base85(n, width=4)
        self.assertEqual(len(enc), 5)
        enc = key_to_base85(n, width=8)
        self.assertEqual(len(enc), 10)

    def test_different_numbers_produce_different_encodings(self):
        enc1 = key_to_base85(123, width=1)
        enc2 = key_to_base85(124, width=1)
        self.assertNotEqual(enc1, enc2)

if __name__ == "__main__":
    unittest.main()
