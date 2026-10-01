import logging
import time

import pika

from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
)

CONNECTION_RETRIES = 10
CONNECTION_RETRY_DELAY = 3


class _MessageMiddlewareRabbitMQ:
    """Common RabbitMQ functionality for the queue and exchange middleware."""

    def _connect(self, host, retries=CONNECTION_RETRIES, delay=CONNECTION_RETRY_DELAY):
        """Connect to RabbitMQ, retrying while the broker is not ready yet.

        Controls may start before RabbitMQ accepts connections, so a failed
        connection attempt is retried up to `retries` times, waiting `delay`
        seconds between attempts. Any other error fails immediately.
        """
        self._connection = None
        for attempt in range(1, retries + 1):
            try:
                self._connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=host)
                )
                self._channel = self._connection.channel()
                return
            except pika.exceptions.AMQPConnectionError as exc:
                self._close_quietly()
                if attempt == retries:
                    raise MessageMiddlewareDisconnectedError() from exc
                logging.warning(
                    f"RabbitMQ not ready (attempt {attempt}/{retries}), "
                    f"retrying in {delay}s"
                )
                time.sleep(delay)
            except Exception as exc:
                self._close_quietly()
                raise MessageMiddlewareMessageError() from exc

    def _close_quietly(self):
        """Close the connection if it is still open, without raising."""
        try:
            if self._connection is not None and self._connection.is_open:
                self._connection.close()
        except Exception:
            pass

    def _ack(self, delivery_tag):
        try:
            self._channel.basic_ack(delivery_tag=delivery_tag)
        except pika.exceptions.AMQPConnectionError as exc:
            raise MessageMiddlewareDisconnectedError() from exc
        except Exception as exc:
            raise MessageMiddlewareMessageError() from exc

    def _nack(self, delivery_tag):
        try:
            self._channel.basic_nack(delivery_tag=delivery_tag)
        except pika.exceptions.AMQPConnectionError as exc:
            raise MessageMiddlewareDisconnectedError() from exc
        except Exception as exc:
            raise MessageMiddlewareMessageError() from exc

    def _consume(self, on_message_callback):
        try:
            self._consuming = True
            # Deliver one message at a time: the next one is not sent until the
            # current one is acked. This spreads the work evenly between
            # replicas sharing a queue and keeps the "FIFO + already processed"
            # assumption used to coordinate the EOF between Sum instances.
            self._channel.basic_qos(prefetch_count=1)
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=self._build_callback(on_message_callback),
            )
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as exc:
            raise MessageMiddlewareDisconnectedError() from exc
        except Exception as exc:
            raise MessageMiddlewareMessageError() from exc
        finally:
            self._consuming = False

    def _build_callback(self, on_message_callback):
        def callback(channel, method, properties, body):
            ack = lambda: self._ack(method.delivery_tag)
            nack = lambda: self._nack(method.delivery_tag)
            on_message_callback(body, ack, nack)

        return callback

    def _stop(self):
        if not self._consuming:
            return

        try:
            self._channel.stop_consuming()
            self._consuming = False
        except pika.exceptions.AMQPConnectionError as exc:
            self._consuming = False
            raise MessageMiddlewareDisconnectedError() from exc
        except Exception as exc:
            raise MessageMiddlewareMessageError() from exc

    def _send(self, exchange, routing_key, message):
        try:
            self._channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=message,
            )
        except pika.exceptions.AMQPConnectionError as exc:
            raise MessageMiddlewareDisconnectedError() from exc
        except Exception as exc:
            raise MessageMiddlewareMessageError() from exc

    def _close(self):
        try:
            if self._connection.is_open:
                self._connection.close()
        except pika.exceptions.AMQPConnectionError as exc:
            raise MessageMiddlewareCloseError() from exc
        except Exception as exc:
            raise MessageMiddlewareCloseError() from exc


class MessageMiddlewareQueueRabbitMQ(_MessageMiddlewareRabbitMQ, MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._consuming = False
        self._connect(host)

        try:
            self._queue_name = queue_name
            self._channel.queue_declare(queue=queue_name)
        except pika.exceptions.AMQPConnectionError as exc:
            self._close_quietly()
            raise MessageMiddlewareDisconnectedError() from exc
        except Exception as exc:
            self._close_quietly()
            raise MessageMiddlewareMessageError() from exc

    def start_consuming(self, on_message_callback):
        self._consume(on_message_callback)

    def stop_consuming(self):
        self._stop()

    def send(self, message):
        self._send("", self._queue_name, message)

    def close(self):
        self._close()


class MessageMiddlewareExchangeRabbitMQ(_MessageMiddlewareRabbitMQ, MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self._consuming = False
        self._queue_name = None
        self._connect(host)

        try:
            self._exchange_name = exchange_name
            self._routing_keys = list(routing_keys)
            self._channel.exchange_declare(
                exchange=exchange_name,
                exchange_type="direct",
            )
        except pika.exceptions.AMQPConnectionError as exc:
            self._close_quietly()
            raise MessageMiddlewareDisconnectedError() from exc
        except Exception as exc:
            self._close_quietly()
            raise MessageMiddlewareMessageError() from exc

    def _ensure_queue(self):
        """Create the exclusive queue and its bindings the first time it is needed.

        Each middleware instance that consumes gets its own exclusive queue.
        This is what makes an exchange broadcast a message to every consumer
        instead of load-balancing messages between consumers. Pure producers
        never create it, so no unconsumed copies pile up for them.
        """
        if self._queue_name is not None:
            return

        try:
            declared_queue = self._channel.queue_declare(
                queue="",
                exclusive=True,
                auto_delete=True,
            )
            queue_name = declared_queue.method.queue

            for routing_key in self._routing_keys:
                self._channel.queue_bind(
                    exchange=self._exchange_name,
                    queue=queue_name,
                    routing_key=routing_key,
                )
            self._queue_name = queue_name
        except pika.exceptions.AMQPConnectionError as exc:
            raise MessageMiddlewareDisconnectedError() from exc
        except Exception as exc:
            raise MessageMiddlewareMessageError() from exc

    def start_consuming(self, on_message_callback):
        self._ensure_queue()
        self._consume(on_message_callback)

    def stop_consuming(self):
        self._stop()

    def send(self, message):
        # Preserve the original middleware contract: send to every configured
        # routing key.
        for routing_key in self._routing_keys:
            self._send(self._exchange_name, routing_key, message)

    def send_to(self, routing_key, message):
        # Producer-side routing without creating one middleware connection per
        # destination. The routing key must be one of those configured when
        # the exchange middleware was created.
        if routing_key not in self._routing_keys:
            raise MessageMiddlewareMessageError()
        self._send(self._exchange_name, routing_key, message)

    def close(self):
        self._close()