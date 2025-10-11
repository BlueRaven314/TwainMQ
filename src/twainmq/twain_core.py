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

topicChars = string.ascii_letters + string.digits + string.punctuation
MessageRootLocation = Path(r"\\LAPTOP-DHA3J8F0\SharedData\Messaging")

class MessageBrokerError(Exception): pass
class PartitionCorruptError(MessageBrokerError): pass
class TopicNotFoundError(MessageBrokerError): pass
class ConfigNotFoundError(FileNotFoundError, MessageBrokerError): pass
class TopicAlreadyCreated(MessageBrokerError): pass
class InvalidTopicCodeSpecification(MessageBrokerError): pass
class InvalidTopicBytesError(MessageBrokerError): pass
class NoActiveMessageFileToReadError(MessageBrokerError): pass

MessageTuple = namedtuple("MessageTuple", ["Offset", "Topic", "Timestamp", "Message"])

class MessageBrokerBase(ABC):
    def __init__(self, partition):
        self.partition = partition
        configPath = _getConfigPath(partition)
        if not configPath.is_file():
            raise ConfigNotFoundError(f"No config for {partition}: cannot find file {configPath}")
        with configPath.open("r") as configFile:
            configData = configFile.read().splitlines()
        if configData[0] not in ["1", "2", "3", "4"]:
            raise InvalidTopicCodeSpecification(f"Do not understand topic config code: {configData[0]}") 
        self._topicBytes = int(configData[0])
        self._topicNames = configData[1:]
        self._topicCodes = generateTopicCodes(self._topicBytes, len(self._topicNames))
        self._topic2code = dict(zip(self._topicNames, self._topicCodes))
        self._code2topic = dict(zip(self._topicCodes, self._topicNames))
        self._brokerCode = self._topicCodes[0]
        self._messageFile, self._todayStr = self._getActiveMessageFile()
        self._currentOffset = None
        self._currentFileHandle = None
        
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self.close()

    @property
    def _messageFilesByOffset(self):
        partitionDir = MessageRootLocation / self.partition
        messageFilesByOffset = [(int(x.stem.split("_")[1]), x) for x in partitionDir.iterdir()]
        return messageFilesByOffset
    
    def _getActiveMessageFile(self):
        partitionDir = MessageRootLocation / self.partition
        todayStr = f"{datetime.utcnow():%Y%m%d}"
        messageFiles = [x for x in partitionDir.iterdir()]
        activeFile = [x for x in messageFiles if x.stem[:8] == todayStr]
        if len(activeFile) == 0:
            if len(messageFiles) == 0:
                fileOffset = 0
            else:
                prevFile = max(messageFiles)
                with Path(prevFile).open("r") as f:
                    prevFileLen = len(f.readlines())
                prevFileOffset = int(prevFile.stem.split("_")[1])
                fileOffset = prevFileOffset + prevFileLen
            newActiveFile = partitionDir / f"{todayStr}_{fileOffset}.mbm"
            self._initNewMessageFile(newActiveFile, fileOffset)
            return newActiveFile, todayStr
        elif len(activeFile) == 1:
            activeFile = Path(activeFile[0])
            return activeFile, todayStr
        else:
            raise PartitionCorruptError(f"Multiple message files for the same day {todayStr}")
    
    def __repr__(self):
        if len(self._topicNames) > 5:
            TopicRepr = f"...{len(self._topicNames) - 1}..."
        else:
            TopicRepr = ", ".join(self._topicNames)
        return f"{self.__class__.__name__}(partition={self.partition}, topics  = [{TopicRepr}])@offset={self.offset}"
        
    def _initNewMessageFile(self, activeFile, offset):
        tcode = self._brokerCode
        msgBlob = encodeMessage(f"Offset={offset}")
        timestamp = encodeDateTime(datetime.utcnow())
        activeFile.write_text(f"{tcode}{timestamp}{msgBlob}\n")
    
    @property
    def _activeFile(self):
        todayStr = f"{datetime.utcnow():%Y%m%d}"
        if todayStr != self._todayStr:
            self._messageFile, self._todayStr = self._getActiveMessageFile()
        return self._messageFile
    
    @property
    def offset(self):
        return self._currentOffset
        
    @property
    def topics(self):
        return list(self._topicNames)
        
    def getTopicCode(self, topic):
        try:
            return self._topic2code[topic]
        except KeyError as e:
            raise TopicNotFoundError(f"Topic '{topic}' does not exist in partition '{self.partition}'") from e  
    def getTopic(self, topicCode):
        return self._code2topic.get(topicCode, "UNKNOWN TOPIC")
        
    def close(self):
        if self._currentFileHandle is not None:
            self._currentFileHandle.close()
        self._currentFileHandle = None
        
class MessageBrokerReader(MessageBrokerBase):
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

class MessageBrokerWriter(MessageBrokerBase):
    def __init__(self, partition):
        super().__init__(partition)
    
    def writeMessage(self, topic, message):
        tcode = self.getTopicCode(topic)
        msgBlob = encodeMessage(message)
        timestamp = encodeDateTime(datetime.utcnow())
        with self._activeFile.open("a") as f:
            f.write(f"{tcode}{timestamp}{msgBlob}\n")

def _getConfigPath(partitionName):
    return (MessageRootLocation / partitionName).with_suffix(".mbc")

def listPartitions():
    return [f.stem for f in MessageRootLocation.glob("*.mbc")]

def createPartition(partitionName, topics = None, topicBytes = 2):
    """Create a new partition"""
    if topics is None:
        topics = []
    if topicBytes not in [1,2,3,4]:
        raise InvalidTopicBytesError("Only 1 to 4 bytes supported")
    if not isinstance(topics, list):
        raise TypeError("Must supply list of topic names or None")
    if len(topics) + 1 > len(topicChars) ** topicBytes:
        raise ValueError(f"Cannot create partition, number of topics ({len(topics)}) exceed topic encoding ({len(topicChars) ** topicBytes})")
    if (MessageRootLocation / partitionName).exists():
        raise ValueError(f"Cannot create partition, {partitionName} already exists")
    newPartDir = (MessageRootLocation / partitionName).mkdir()
    configPath = _getConfigPath(partitionName)
    with configPath.open("w") as newConfig:
        newConfig.write(f"{topicBytes}\n")
    addTopics(partitionName, ["RESERVED"] + topics)
    
def addTopics(partitionName, topics):
    """Add topics"""
    if not isinstance(topics, list):
        raise TypeError("Must supply list of topic names")
    
    configPath = _getConfigPath(partitionName)
    if not configPath.is_file():
        raise ConfigNotFoundError(f"No config for {partitionName}: cannot find file {configPath}")
    
    # Check that topics are not already in config
    #
    #
    #
    #
    #
    #
    #
    
    
    with configPath.open("a") as f:
        for thisTopic in topics:
        
        
            f.write(thisTopic + "\n")

_bytesFlag = b"\x99"   # sentinal added as first byte of message to indicate message is raw bytes (will not encode into utf-8)

def encodeDateTime(dt):
    """Return 10 byte encoded date string"""
    return base64.b85encode(np.array(dt.timestamp()).tobytes()).decode("utf-8")
    
def decodeDateTime(dt):
    return datetime.fromtimestamp(np.frombuffer(base64.b85decode(dt.encode("utf-8")))[0])

import base64

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
    
def generateTopicCodes(topicBytes, nTopics):
    topicList = []
    n = len(topicChars)
    for x in range(nTopics):
        topicCode = ""
        for i in range(topicBytes):
            topicCode += topicChars[x % n]
            x = x // n
        topicList.append(topicCode)
    return topicList

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
