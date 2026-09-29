import pika

from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
)


class _MessageMiddlewareRabbitMQ:
    """Common RabbitMQ functionality for the queue and exchange middleware."""

    def _connect(self, host):
        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
            self._channel = self._connection.channel()
        except pika.exceptions.AMQPConnectionError as exc:
            raise MessageMiddlewareDisconnectedError() from exc
        except Exception as exc:
            raise MessageMiddlewareMessageError() from exc

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
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=self._build_callback(on_message_callback),
            )
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as exc:
            self._consuming = False
            raise MessageMiddlewareDisconnectedError() from exc
        except Exception as exc:
            self._consuming = False
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
            self._connection.close()
            raise MessageMiddlewareDisconnectedError() from exc
        except Exception as exc:
            self._connection.close()
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
        self._connect(host)

        try:
            self._exchange_name = exchange_name
            self._routing_keys = list(routing_keys)
            self._channel.exchange_declare(
                exchange=exchange_name,
                exchange_type="direct",
            )

            # Each middleware instance gets its own exclusive queue. This is
            # what makes an exchange broadcast a message to every consumer
            # instead of load-balancing messages between consumers.
            declared_queue = self._channel.queue_declare(
                queue="",
                exclusive=True,
                auto_delete=True,
            )
            self._queue_name = declared_queue.method.queue

            for routing_key in routing_keys:
                self._channel.queue_bind(
                    exchange=exchange_name,
                    queue=self._queue_name,
                    routing_key=routing_key,
                )
        except pika.exceptions.AMQPConnectionError as exc:
            self._connection.close()
            raise MessageMiddlewareDisconnectedError() from exc
        except Exception as exc:
            self._connection.close()
            raise MessageMiddlewareMessageError() from exc

    def start_consuming(self, on_message_callback):
        self._consume(on_message_callback)

    def stop_consuming(self):
        self._stop()

    def send(self, message):
        # A single middleware instance may be initialized with several routing
        # keys. A message is sent using each key, so it reaches every queue
        # bound to that key.
        for routing_key in self._routing_keys:
            self._send(self._exchange_name, routing_key, message)

    def close(self):
        self._close()