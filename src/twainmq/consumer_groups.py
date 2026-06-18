from dataclasses import dataclass
from datetime import timedelta
from dataclasses_jsonschema import JsonSchemaMixin

from .errors import InvalidGroupNameError
from .twain_core import _is_safe


REBAL_LENGTH = timedelta(seconds = 60)

## CONSUMER GROUP MESSAGES

@dataclass
class Joined(JsonSchemaMixin):
    tag: int
    __message_type__ = "~~TWAIN~~JOINED"
    
    def __init__(self, tag):
        self.tag = tag

@dataclass
class AbortJoin(JsonSchemaMixin):
    tag: int
    __message_type__ = "~~TWAIN~~ABORTJOIN"
    
    def __init__(self, tag):
        self.tag = tag

@dataclass
class RebalOffer(JsonSchemaMixin):
    own: list[int]
    partitions: list[int]
    __message_type__ = "~~TWAIN~~REBALOFFER"

    def __init__(self, own, partitions):
        self.own = own
        self.partitions = partitions

@dataclass
class RebalConfirm(JsonSchemaMixin):
    partitions: list[int]
    __message_type__ = "~~TWAIN~~REBALCONFIRM"

    def __init__(self, partitions):
        self.partitions = partitions

@dataclass
class BeginRebal(JsonSchemaMixin):
    timestamp: float
    __message_type__ = "~~TWAIN~~BEGINREBAL"

    def __init__(self, timestamp):
        self.timestamp = timestamp

@dataclass
class EndRebal(JsonSchemaMixin):
    __message_type__ = "~~TWAIN~~ENDREBAL"

    def __init__(self):
        pass

@dataclass
class Commit(JsonSchemaMixin):
    partition_offsets: list[tuple[int, int]]
    __message_type__ = "~~TWAIN~~COMMIT"

    def __init__(self, partition_offsets):
        self.partition_offsets = partition_offsets

CONSUMER_GROUP_MESSAGE_CLASSES = [Joined, AbortJoin, RebalOffer, RebalConfirm, BeginRebal, EndRebal, Commit]
CONSUMER_GROUP_MESSAGE_SET = [c.__message_type__ for c in CONSUMER_GROUP_MESSAGE_CLASSES]

def _group_topic_name(group):
    if not _is_safe(group):
        raise InvalidGroupNameError(f"Consumer group names must be [a-zA-Z0-9_.], {group} is not valid")
    return f"--group--{group}"

class RebalInProgress:
    def __init__(self, rebal_ending):
        self.rebal_ending = rebal_ending
        self._offers = dict()

    def add_offer(self, cons_id: int, rebal_offer: RebalOffer):
        self._offers[cons_id] = rebal_offer

    def get_assignments(self):
        # --- Step 0: Clean ownership lists ---
        cleaned = []
        for cid, offer in self._offers.items():
            allowed = set(offer.partitions)
            owned = [p for p in offer.own if p in allowed]
            cleaned.append((cid, allowed, owned))

        # --- Step 1: Sort each consumer's partition list ---
        # Owned first, then by partition ID
        consumer_lists = {}
        for cid, allowed, owned in cleaned:
            owned_sorted = sorted(owned)
            new_sorted = sorted(p for p in allowed if p not in owned)
            consumer_lists[cid] = owned_sorted + new_sorted

        # --- Step 2: Sort consumers ---
        # a) number of partitions offered (ascending)
        # b) number of partitions owned (descending)
        # c) consumer id (ascending)
        sorted_consumers = sorted(
            cleaned,
            key=lambda x: (len(x[1]), -len(x[2]), x[0])
        )

        # --- Step 3: Round-robin assignment ---
        assigned = {cid: [] for cid, _, _ in cleaned}
        remaining_lists = {cid: list(consumer_lists[cid]) for cid, _, _ in cleaned}

        # Set of all partitions that appear anywhere
        all_partitions = set()
        for _, allowed, _ in cleaned:
            all_partitions.update(allowed)

        assigned_partitions = set()

        # Continue until all partitions assigned
        while len(assigned_partitions) < len(all_partitions):
            for cid, allowed, owned in sorted_consumers:
                lst = remaining_lists[cid]

                # Skip already-taken partitions
                while lst and lst[0] in assigned_partitions:
                    lst.pop(0)

                if not lst:
                    continue

                # Assign the next partition
                p = lst.pop(0)
                assigned[cid].append(p)
                assigned_partitions.add(p)

        # Sort results for determinism
        for cid in assigned:
            assigned[cid].sort()

        return assigned
