import os
import signal
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        # By client: partial tops received from Aggregation replicas.
        self.pending = {}

    def _process_aggregation_result(self, client_id, aggregation_id, items):
        state = self.pending.setdefault(
            client_id, {"items": [], "aggregation_ids": set()}
        )
        if aggregation_id in state["aggregation_ids"]:
            return
        state["items"].extend(items)
        state["aggregation_ids"].add(aggregation_id)

        if len(state["aggregation_ids"]) == AGGREGATION_AMOUNT:
            self._flush_client(client_id)

    def _flush_client(self, client_id):
        logging.info(f"Join: flushing client {client_id}")
        state = self.pending.pop(client_id)
        top_items = [
            fruit_item.FruitItem(fruit, amount)
            for fruit, amount in state["items"]
        ]
        top_items.sort()
        top_items.reverse()
        top_items = top_items[:TOP_SIZE]

        self.output_queue.send(
            message_protocol.internal.serialize(
                {
                    "client_id": client_id,
                    "items": [[fi.fruit, fi.amount] for fi in top_items],
                }
            )
        )

    def process_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        self._process_aggregation_result(
            fields["client_id"], fields["aggregation_id"], fields["items"]
        )
        ack()

    def stop(self):
        self.input_queue.stop_consuming()

    def close(self):
        self.input_queue.close()
        self.output_queue.close()

    def start(self):
        self.input_queue.start_consuming(self.process_message)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()

    def handle_sigterm(signum, frame):
        logging.info("Recieved SIGTERM signal")
        join_filter.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)

    join_filter.start()
    join_filter.close()
    return 0


if __name__ == "__main__":
    main()
