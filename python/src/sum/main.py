import os
import signal
import logging
import threading
import zlib

from common import middleware, message_protocol

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

# EOF is broadcast to every Sum. Each Sum flushes only the data it actually
# processed for that client and then emits one completion message. Aggregations
# use the completion messages as a barrier.
SUM_CONTROL_EXCHANGE = f"{SUM_PREFIX}_control"
SUM_CONTROL_ROUTING_KEY = "eof"


def _aggregation_id_for(fruit):
    """Deterministically assign each fruit to exactly one Aggregation."""
    return zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT


class SumFilter:
    def __init__(self):
        self.id = ID
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        # One producer exchange/channel per Sum instead of one connection per
        # Sum/Aggregation pair. send_to() selects the destination routing key.
        self.aggregation_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            AGGREGATION_PREFIX,
            [f"{AGGREGATION_PREFIX}_{i}" for i in range(AGGREGATION_AMOUNT)],
        )

        self.control_out = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_ROUTING_KEY]
        )
        self.control_in = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_ROUTING_KEY]
        )

        self.amount_by_client = {}
        self.lock = threading.Lock()
        self._control_thread = None

    def _process_data(self, client_id, fruit, amount):
        with self.lock:
            client_state = self.amount_by_client.setdefault(client_id, {})
            client_state[fruit] = client_state.get(fruit, 0) + int(amount)

    def _flush_client(self, client_id):
        with self.lock:
            amount_by_fruit = self.amount_by_client.pop(client_id, {})

        logging.info(f"Sum {self.id}: flushing client {client_id}")

        # Only send a data message to an Aggregation when this Sum has data for
        # that partition. This removes the previous SumAmount x AggregationAmount
        # empty-message traffic.
        buckets = {}
        for fruit, amount in amount_by_fruit.items():
            aggregation_id = _aggregation_id_for(fruit)
            buckets.setdefault(aggregation_id, []).append([fruit, amount])

        for aggregation_id, items in buckets.items():
            self.aggregation_exchange.send_to(
                f"{AGGREGATION_PREFIX}_{aggregation_id}",
                message_protocol.internal.serialize(
                    {
                        "type": "data",
                        "client_id": client_id,
                        "sum_id": self.id,
                        "items": items,
                    }
                ),
            )

        # Every Aggregation must know that this Sum has finished. The completion
        # message is sent on the SAME routing key/channel as that Aggregation's
        # data. Therefore RabbitMQ preserves the order data -> done for this
        # Sum, avoiding a cross-channel race at the Aggregation.
        done_message = message_protocol.internal.serialize(
            {
                "type": "sum_done",
                "client_id": client_id,
                "sum_id": self.id,
                "items": [],
            }
        )
        for aggregation_id in range(AGGREGATION_AMOUNT):
            self.aggregation_exchange.send_to(
                f"{AGGREGATION_PREFIX}_{aggregation_id}", done_message
            )

    def _on_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        self._flush_client(fields["client_id"])
        ack()

    def _on_input_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if fields["type"] == "data":
            self._process_data(fields["client_id"], fields["fruit"], fields["amount"])
        else:
            # The Sum that receives EOF broadcasts it to all Sum replicas. The
            # control callback is independent from the input consumer, so the
            # latter keeps draining its current message before the next one is
            # acknowledged. prefetch_count=1 bounds this in-flight work.
            self.control_out.send(
                message_protocol.internal.serialize(
                    {"client_id": fields["client_id"]}
                )
            )
        ack()

    def stop(self):
        self.input_queue.stop_consuming()
        self.control_in.stop_consuming()

    def close(self):
        self.input_queue.close()
        self.control_out.close()
        self.control_in.close()
        self.aggregation_exchange.close()

    def start(self):
        self._control_thread = threading.Thread(
            target=lambda: self.control_in.start_consuming(self._on_control_message),
            daemon=True,
        )
        self._control_thread.start()
        self.input_queue.start_consuming(self._on_input_message)


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()

    def handle_sigterm(signum, frame):
        logging.info("Recieved SIGTERM signal")
        sum_filter.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)

    sum_filter.start()
    sum_filter.close()
    return 0


if __name__ == "__main__":
    main()
