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

"""
All instances of Sum are bound to this SAME routing key of the
control exchange: since it's a 'direct' exchange with multiple queues
bound with the same key, a publish reaches ALL instances
(broadcast), regardless of which one actually
read the client's EOF from the shared input queue.
"""
SUM_CONTROL_EXCHANGE = f"{SUM_PREFIX}_control"
SUM_CONTROL_ROUTING_KEY = "eof"


def _aggregation_id_for(fruit):
    """
    Deterministic hash (process-independent) so that all
    instances of Sum match which Aggregation each
    fruit belongs to, and so each fruit goes to a single Aggregation (no broadcast).
    """
    return zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT


class SumFilter:
    def __init__(self):
        self.id = ID
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.aggregation_exchanges = {
            i: middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            for i in range(AGGREGATION_AMOUNT)
        }
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
        buckets = {i: [] for i in range(AGGREGATION_AMOUNT)}
        for fruit, amount in amount_by_fruit.items():
            buckets[_aggregation_id_for(fruit)].append([fruit, amount])

        for aggregation_id, items in buckets.items():
            self.aggregation_exchanges[aggregation_id].send(
                message_protocol.internal.serialize(
                    {
                        "client_id": client_id,
                        "sum_id": self.id,
                        "items": items,
                    }
                )
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
            # Avisamos a TODAS las instancias de Sum (incluida esta misma)
            # que el cliente termino, para que cada una vuelque lo que
            # haya acumulado localmente para ese cliente.
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
        for exchange in self.aggregation_exchanges.values():
            exchange.close()

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
