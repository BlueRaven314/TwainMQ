
import base64
import bisect
from datetime import datetime, timezone
import json
import random
import time
import zlib

from twainmq.encoding import _DATACLASS_MAGIC, _GZIP_MAGIC, base85_to_key, dataclass_from_dict, decode_datetime
from twainmq.errors import TopicCorruptError

from .consumer_groups import CONSUMER_GROUP_MESSAGE_SET, REBAL_LENGTH, AbortJoin, BeginRebal, Commit, EndRebal, Joined, RebalConfirm, RebalInProgress, RebalOffer, _group_topic_name
from .core import MessageTuple, Twain, TwainMQBase


class TwainMQConsumer(TwainMQBase):
    """A consumer
    
    A consumer does not need to be part of a consumer group, but if it is not, then it will not be able to commit.

    If a consumer is part of a group (even a group of one) then it can commit to record where it got to and then a consumer (re-)joining that group will pick up
    from where it left off

    `start_from` is only used when there is no consumer group, or when no commit has been made in a group.
    """
    def __init__(self, twain: Twain, topic: str, start_from = "start", group = None):
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
        if not self._twain.topic_exists(group_topic):
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
            elif isinstance(msg.message, Joined):
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

