
import random


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