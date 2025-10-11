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

class MessageBrokerError(Exception): pass
class TopicCorruptError(MessageBrokerError): pass
class ConfigNotFoundError(FileNotFoundError, MessageBrokerError): pass
class TopicAlreadyExists(MessageBrokerError): pass
class NoActiveMessageFileToReadError(MessageBrokerError): pass

MessageTuple = namedtuple("MessageTuple", ["Offset", "Key", "Timestamp", "Message"])

class TwainMQBase(ABC):
    def __init__(self, twain_directory, topic):
        self.topic = topic
        self.twain_directory = Path(twain_directory)
        config_path = _getConfigPath(partition)
        if not configPath.is_file():
            raise ConfigNotFoundError(f"No config for {topic}: cannot find file {config_path}")
        self._message_file, self._todayStr = self._get_active_message_file()
        self._current_offset = None
        self._current_file_handle = None
        
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self.close()

    @property
    def _message_files_by_offset(self):
        topic_dir = self.twain_directory / self.topic
        message_files_by_offset = [(int(x.stem.split("_")[1]), x) for x in topic_dir.iterdir()]
        return message_files_by_offset
    
    def _get_active_message_file(self):
        topic_dir = self.twain_directory / self.topic
        chunk_str = f"{datetime.utcnow():%Y%m%d}"   ## Daily chunks for now
        message_files = list(topic_dir.iterdir())
        active_file = [x for x in message_files if x.stem[:8] == chunk_str]
        if len(active_file) == 0:
            if len(message_files) == 0:
                file_offset = 0
            else:
                prev_file = max(message_files)
                with Path(prev_file).open("r") as f:
                    prev_file_len = len(f.readlines())
                prev_file_offset = int(prev_file.stem.split("_")[1])
                file_offset = prev_file_offset + prev_file_len
            new_active_file = topic_dir / f"{chunk_str}_{file_offset}.tmf"
            self._init_new_message_file(new_active_file, file_offset)
            return new_active_file, chunk_str
        elif len(active_file) == 1:
            active_file = Path(active_file[0])
            return active_file, chunk_str
        else:
            raise TopicCorruptError(f"Multiple message files for the same chunk {chunk_str}")
    
    def __str__(self):
        return f"{self.__class__.__name__}(topic={self.topic}@offset={self.offset}"
        
    def _init_new_message_file(self, active_file, offset):
        pass
    
    @property
    def _active_file(self):
        chunk_str = f"{datetime.utcnow():%Y%m%d}"
        if chunk_str != self._chunk_str:
            self._message_file, self._chunk_str = self._get_active_message_file()
        return self._message_file
    
    @property
    def offset(self):
        return self._current_offset

    def close(self):
        if self._current_file_handle is not None:
            self._current_file_handle.close()
        self._current_file_handle = None
        
class TwainMQConsumer(MessageBrokerBase):
    def __init__(self, partition, topicFilter = None, offset = None):
        super().__init__(partition)
        if topicFilter is None:
            self._topicFilterCodes = self._topicCodes[1:]
        elif topicFilter == "RESERVED":
            self._topicFilterCodes = set([self._brokerCode])
        else:
            self._topicFilterCodes = set(self.getTopicCode(t) for t in topicFilter)
            
        if offset is None:
            self._currentOffset = int(self._activeFile.stem.split("_")[1])
            self._setCurrentFileHandle()
            self.nextFileStart = None
        elif offset == -1:   # Last offset - in future special case of specific offsets back (which need seek)
            self._currentOffset = int(self._activeFile.stem.split("_")[1])
            self._setCurrentFileHandle()
            self.nextFileStart = None
            self.readAllMessages()
        elif offset < -1:
            raise NotImplementedError("Cannot do specific offsets back from end yet")
        else:
            self.nextFileStart = None
            for o, f in self._messageFilesByOffset:
                if o > offset:
                    self.nextFileStart = o
                    break
                else:
                    readingFilePath = f
            self._currentFileHandle = readingFilePath.open("r")
            self._currentOffset = int(readingFilePath.stem.split("_")[1])

    def _setCurrentFileHandle(self):
        if self._activeFile.exists():
            self._currentFileHandle = self._activeFile.open("r")
        else:
            raise NoActiveMessageFileToReadError(f"Expected broker file {self._activeFile} does not exist yet")

    def _initNewMessageFile(self, activeFile, offset):
        print("Not initialising - read only")
        pass
            
    def readAllMessages(self):
        todayStr = f"{datetime.utcnow():%Y%m%d}"
        newLines = self._currentFileHandle.read().splitlines()
        offsetsRead = len(newLines)
        filteredLines = [(i, L) for i, L in enumerate(newLines) if L[:self._topicBytes] in self._topicFilterCodes]
        messages = [MessageTuple(Offset = self._currentOffset + i,
                                 Topic = self.getTopic(fl[:self._topicBytes]),
                                 Timestamp = decodeDateTime(fl[self._topicBytes:10+self._topicBytes]),
                                 Message = decodeMessage(fl[10+self._topicBytes:])
                                 )
                    for i, fl in filteredLines]
        self._currentOffset += offsetsRead
        if self._currentOffset == self.nextFileStart:
            self._currentFileHandle.close()
            readingFilePath = dict(self._messageFilesByOffset)[self.nextFileStart]
            self._currentFileHandle = readingFilePath.open("r")
            self.nextFileStart = None
            for o, f in self._messageFilesByOffset:
                if o > self._currentOffset:
                    self.nextFileStart = o
                    break
            messages += self.readAllMessages()
        if self.nextFileStart is None:
            if todayStr > self._todayStr:
                self._currentFileHandle.close()
                self._currentOffset = int(self._activeFile.stem.split("_")[1])
                self._currentFileHandle = self._activeFile.open("r")
                self.nextFileStart = None
        return messages

class TwainMQProducer(MessageBrokerBase):
    def __init__(self, topic):
        super().__init__(topic)
    
    def writeMessage(self, key, message):
        encoded_key = int_to_base85(key)
        msgBlob = encodeMessage(message)
        timestamp = encodeDateTime(datetime.utcnow())
        with self._activeFile.open("a") as f:
            f.write(f"{tcode}{timestamp}{msgBlob}\n")

def _getConfigPath(partitionName):
    return (MessageRootLocation / partitionName).with_suffix(".mbc")

def listTopic():
    return [f.stem for f in MessageRootLocation.glob("*.mbc")]

def createTopic(topicName, keyBytes = 2):
    """Create a new topic"""
    if keyBytes > 8:
        raise InvalidTopicBytesError("Key must be 8 bytes or fewer")
    if (MessageRootLocation / topicName).exists():
        raise ValueError(f"Cannot create topic, {topicName} already exists")
    newPartDir = (MessageRootLocation / topicName).mkdir()
    configPath = _getConfigPath(topicName)
    with configPath.open("w") as newConfig:
        newConfig.write(f"{keyBytes}\n")
    
_bytesFlag = b"\x99"   # sentinal added as first byte of message to indicate message is raw bytes (will not encode into utf-8)
## Anything in the range \x80 to \xBF ought to be safe to use as sentinal bytes

def encodeDateTime(dt):
    """Return 10 byte encoded date string"""
    return base64.b85encode(np.array(dt.timestamp()).tobytes()).decode("utf-8")
    
def decodeDateTime(dt):
    return datetime.fromtimestamp(np.frombuffer(base64.b85decode(dt.encode("utf-8")))[0])

def encodeMessage(message):
    if isinstance(message, bytes):
        payload = _bytesFlag + message
    else:
        payload = message.encode("utf-8")
    # Need to test to optimise the compression rate
    compressed = zlib.compress(payload, level=6, wbits=-15)
    return base64.b85encode(compressed).decode("utf-8")

def decodeMessage(message):
    compressed = base64.b85decode(message.encode("utf-8"))
    decoded = zlib.decompress(compressed, wbits=-15)
    if decoded.startswith(_bytesFlag):
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

def base85_to_int(s: str, width: int) -> int:
    """
    Decode a Base85 string back into an unsigned integer.
    """
    b = base64.b85decode(s.encode("ascii"))
    return int.from_bytes(b, byteorder="big", signed=False)

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
