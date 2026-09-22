
import random

def multi_poll(consumers, offset=None):
    """
    Poll multiple TwainMQ consumers in a fair, round-robin sequence.

    TwainMQ consumers can only subscribe to a single topic, so applications that
    need to consume from multiple topics typically create multiple consumer
    objects. `multi_poll` provides a simple way to poll them evenly.

    Parameters
    ----------
    consumers : list
        A list of consumer objects, each exposing a `.poll()` method.
    offset : int or None, optional
        The index of the last consumer that was polled. If provided, polling
        resumes from the next consumer in round-robin order. If `None`, a random
        starting offset is chosen.

    Returns
    -------
    (int, message) or None
        Returns a tuple `(index, msg)` where `index` is the consumer that
        produced a message and `msg` is the message itself. Returns `None` if no
        consumer produced a message.

    Notes
    -----
    - To achieve continuous round-robin behaviour, store the returned index and
      pass it back as `offset` on the next call.
    - Only the first consumer that returns a non-`None` message is reported.
    """
    n = len(consumers)
    if offset is None:
        offset = random.randrange(n)    
    for c_offset in range(n):
        consumer_to_poll = consumers[(c_offset + offset + 1) % n]
        msg = consumer_to_poll.poll()
        if msg is not None:
            return ((c_offset + offset + 1) % n, msg)