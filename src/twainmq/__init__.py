"""

TwainMQ is a broker free messaging system.  Unlike similar tools like Apache Kafka and RabbitMQ, TwainMQ does not have any running services or brokers to coordinate messages, being run entirely from files.  Think of it like SQLite is to traditional database systems.

To set up a TwainMQ message queue you simply need to specify a directory for it to use, either locally or on a network share.

Messages are limited in size to ensure that they can be written quickly.  The size limit is currently set at slightly less than 4kb.

Messages can be either plain UTF-8 or raw bytes.

Messages are organised by key, partition and topic.

## Keys

u8
u16
u32
u64
str

"""

from .core import Twain, TwainMQConsumer, TwainMQProducer
from .utils import multi_poll