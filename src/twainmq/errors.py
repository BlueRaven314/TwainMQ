
class TwainMQError(Exception): pass
class TopicCorruptError(TwainMQError): pass
class ConfigNotFoundError(TwainMQError): pass
class InvalidTopicNameError(ValueError, TwainMQError): pass
class InvalidGroupNameError(ValueError, TwainMQError): pass
class TopicAlreadyExists(TwainMQError): pass
class NoActiveMessageFileToReadError(TwainMQError): pass
class InvalidKeyTypeError(TwainMQError): pass
class InvalidMessageKeyError(TwainMQError): pass
class TopicDeleteError(TwainMQError): pass
class TwainSchemaError(TwainMQError): pass
class NoSchemaError(TwainSchemaError): pass
class MessageTooLongError(TwainMQError): pass