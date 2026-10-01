import os
import signal
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

class AggregationFilter:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.pending = {}
        self.lock = threading.Lock()

    def _process_message(self, client_id, sum_id, message_type, items):
        should_flush = False
        with self.lock:
            state = self.pending.setdefault(
                client_id, {"amounts": {}, "done_sums": set()}
            )
            if message_type == "data":
                for fruit, amount in items:
                    state["amounts"][fruit] = state["amounts"].get(fruit, 0) + amount
            elif message_type == "sum_done":
                state["done_sums"].add(sum_id)
                should_flush = len(state["done_sums"]) == SUM_AMOUNT

        if should_flush:
            self._flush_client(client_id)

    def _flush_client(self, client_id):
        logging.info(f"Aggregation {ID}: flushing client {client_id}")
        with self.lock:
            state = self.pending.pop(client_id, {"amounts": {}})
            amounts = state["amounts"]

        top_items = [
            fruit_item.FruitItem(fruit, amount)
            for fruit, amount in amounts.items()
        ]
        top_items.sort()
        top_items.reverse()
        top_items = top_items[:TOP_SIZE]

        self.output_queue.send(
            message_protocol.internal.serialize(
                {
                    "client_id": client_id,
                    "aggregation_id": ID,
                    "items": [[fi.fruit, fi.amount] for fi in top_items],
                }
            )
        )

    def process_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        self._process_message(
            fields["client_id"],
            fields["sum_id"],
            fields["type"],
            fields.get("items", []),
        )
        ack()

    def stop(self):
        self.input_exchange.stop_consuming()

    def close(self):
        self.input_exchange.close()
        self.output_queue.close()

    def start(self):
        self.input_exchange.start_consuming(self.process_message)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()

    def handle_sigterm(signum, frame):
        logging.info("Recieved SIGTERM signal")
        aggregation_filter.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)

    aggregation_filter.start()
    aggregation_filter.close()
    return 0


if __name__ == "__main__":
    main()
