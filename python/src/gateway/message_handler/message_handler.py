import uuid

from common import message_protocol


class MessageHandler:
    """
    Connect the external protocol (via TCP client) with the internal protocol (queues/exchanges). 
    Each instance is created once per client connection (see gateway/main.py), 
    so we use that moment to assign it a unique identifier: 
    that ID travels with every internal message to distinguish the data/results of concurrent clients that share the same queues and 
    the same controls (Sum/Aggregation replicated).
    """

    def __init__(self):
        self.client_id = uuid.uuid4().hex

    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize(
            {
                "type": "data",
                "client_id": self.client_id,
                "fruit": fruit,
                "amount": amount,
            }
        )

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize(
            {"type": "eof", "client_id": self.client_id}
        )

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if fields.get("client_id") != self.client_id:
            return None
        return fields.get("items", [])
