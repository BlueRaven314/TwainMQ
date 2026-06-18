
import base64
from dataclasses import asdict, is_dataclass
from datetime import datetime
import json
import zlib

from .atomic_append import atomic_append
from .errors import MessageTooLongError, TopicCorruptError

from .encoding import _DATACLASS_MAGIC, _GZIP_MAGIC, encode_datetime, key_to_base85, partition_hash64
from .core import MAX_MESSAGE_SIZE, TwainMQBase

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