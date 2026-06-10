# Keys

Every message must have a message key when it is published.  The key must be a specific type set when the topic is created.  Currently keys can be either unsigned integer or string, but they must have a fixed byte width.  The default for a topic if not specified is "u16".

Currently allowed types are u8, u16, u32, u64, char1, char2, char4, char8 and char16.

## Parition allocation

Messages are scattered across partitions based on their key, so that two messages with the same key always end up on the same partition.  In order to attempt to balance the distribution, keys are hashed using SplitMix64 and the result used to select the topic.