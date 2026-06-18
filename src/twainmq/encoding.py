
## Anything in the range \x80 to \xBF ought to be safe to use as sentinal magic bytes
import base64
from dataclasses import fields, is_dataclass
from datetime import datetime
import struct

from .errors import InvalidMessageKeyError


_MULTIPART_START = b"\x80"
_MULTIPART_CONTINUE = b"\x81"
_MULTIPART_END = b"\x82"
_DATACLASS_MAGIC = b"\x98"
_GZIP_MAGIC = b"\x99"

def encode_datetime(dt):
    """Return 10 byte encoded date string"""
    ts = dt.timestamp()
    return base64.b85encode(struct.pack("!d", ts)).decode("utf-8")

def decode_datetime(s):
    ts = struct.unpack("!d", base64.b85decode(s.encode("utf-8")))[0]
    return datetime.fromtimestamp(ts)

def dataclass_from_dict(cls, data):
    kwargs = {}
    for f in fields(cls):
        value = data[f.name]
        if is_dataclass(f.type):
            value = dataclass_from_dict(f.type, value)
        kwargs[f.name] = value
    return cls(**kwargs)

def key_to_base85(k, width: int) -> str:
    """
    Encode the key as a base85 string
    """
    if width < 0:
        b = k.encode("utf-8")
        if len(b) > -width:
            raise InvalidMessageKeyError(f"Key {k} too long for {-width} byte string")
        b = b.ljust(-width, b"\0")
    elif width > 0:
        try:
            b = k.to_bytes(width, byteorder="big", signed=False)
        except OverflowError:
            raise InvalidMessageKeyError(f"Integer key too large for {width} bytes")
    else:
        raise NotImplementedError("Zero width keys not yet supported")
    encoded = base64.b85encode(b)
    return encoded.decode("ascii")

def base85_to_key(s: str, width: int):
    """
    Decode a Base85 string back into an key.
    """
    b = base64.b85decode(s.encode("ascii"))
    if width < 0:
        return b.rstrip(b"\0").decode("utf-8")
    elif width > 0:
        return int.from_bytes(b, byteorder="big", signed=False)
    else:
        raise NotImplementedError("Zero width keys not yet supported")

def find_key_char_width(width) -> int:
    """Find the width of the key when encoded"""
    if width > 0:
        return len(key_to_base85(1, width = width))
    elif width < 0:
        return len(key_to_base85("a", width = width))
    else:
        raise NotImplementedError("Zero width keys not yet supported")

def partition_hash64(x, partitions) -> int:
    """Using splitmix64 to convert the key into a partition number for even mixing."""
    if isinstance(x, str):
        x = int.from_bytes(x.encode("utf-8"), signed = False)
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
    x = (x ^ (x >> 27)) * 0x94d049bb133111eb
    x = x ^ (x >> 31)
    return x % partitions