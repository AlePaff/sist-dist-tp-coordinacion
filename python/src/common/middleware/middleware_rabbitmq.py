import pika
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError,
)

# errores de pika que se condieran "desconexión"
_DISCONNECTED_ERRORS = (
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.StreamLostError,
    pika.exceptions.ConnectionClosedByBroker,
)


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try:
            # abre una conexion TCP/AMQP con RabbitMQ.
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )

            # conexion con el receiver a traves de un channel
            self.channel = self.connection.channel()
            self.queue_name = queue_name

            self.channel.queue_declare(
                queue=queue_name,
                # durable=True,               # se persiste en disco, sobrevive reboots de rabbitmq
            )
        # manejo de errores, desde el mas especifico al más generico
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"No se pudo conectar a RabbitMQ en {host}"
            ) from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(
                f"Error al inicializar la queue {queue_name}"
            ) from e


    def start_consuming(self, on_message_callback):
        def _rabbit_callback(channel, method, properties, body):

            def ack():
                try:
                    self.channel.basic_ack(
                        delivery_tag=method.delivery_tag
                    )
                except _DISCONNECTED_ERRORS as e:
                    raise MessageMiddlewareDisconnectedError(f"Se perdió la conexión al hacer ack en la queue {self.queue_name}") from e
                except pika.exceptions.AMQPError as e:
                    raise MessageMiddlewareMessageError(f"Error al hacer ack en la queue {self.queue_name}") from e


            def nack():
                try: 
                    self.channel.basic_nack(
                        delivery_tag=method.delivery_tag
                    )
                except _DISCONNECTED_ERRORS as e:
                    raise MessageMiddlewareDisconnectedError(f"Se perdió la conexión al hacer nack en la queue {self.queue_name}") from e
                except pika.exceptions.AMQPError as e:
                    raise MessageMiddlewareMessageError(f"Error al hacer nack en la queue {self.queue_name}") from e

            on_message_callback(body, ack, nack)
        try:
            # ejecuta el callback cuando llega un mensaje
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=_rabbit_callback,
                auto_ack=False      # no lo confirma automaticamnete, sino que usa las funciones ack y nack
            )
            self.channel.start_consuming()
        except _DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError("Se perdió la conexión mientras se consumía") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError("Error interno durante el consumo") from e

    
    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except _DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(f"Se perdió la conexión al detener el consumo de la queue {self.queue_name}") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error al detener el consumo de la queue {self.queue_name}") from e

    
    def send(self, message):
        try: 
            # publico el mensaje recibido por parametro
            self.channel.basic_publish(
                exchange="",        # exchange por defecto: direct
                routing_key=self.queue_name,        # envia solo a esta queue
                body=message
            )
        except _DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(f"Se perdió la conexión al enviar mensaje a la queue {self.queue_name}") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error al enviar mensaje a la queue {self.queue_name}") from e


    def close(self):
        try: 
            if self.channel.is_open:
                # cerramos el canal y luego la conexion
                self.channel.close()
            if self.connection.is_open:
                self.connection.close()
                # no destruyo la cola
        except _DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareCloseError("Error al cerrar la conexión") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError("Error al cerrar la conexión. General") from e

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        if not routing_keys:
            raise MessageMiddlewareMessageError("Se requiere al menos una routing key")
        
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys        # aplica solo para el consumidor

        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )

            self.channel = self.connection.channel()
            self.channel.exchange_declare(
                exchange=exchange_name,
                exchange_type="direct"      # es el default, el exchange entrega el mensaje a las colas cuyos bindings tienen una routing_key exactamente igual a la routing_key del mensaje.
            )
            # genero una queue
            result = self.channel.queue_declare(
                queue="",
                exclusive=True      # exlusive=True. una vez se cierre este programa, matar la cola
            )
            # guardo la cola
            self.queue_name = result.method.queue

            # bindeo cada cola al exchange, mediante la routing key pasada por parametro
            for routing_key in routing_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=self.queue_name,
                    routing_key=routing_key
                )
        except _DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(f"No se pudo conectar a RabbitMQ en {host} para el exchange {exchange_name}") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error al inicializar el exchange {exchange_name} con routing keys {routing_keys}") from e

    def start_consuming(self, on_message_callback):
        def _rabbit_callback(channel, method, properties, body):
            def ack():
                try:
                    self.channel.basic_ack(
                        delivery_tag=method.delivery_tag
                    )
                except _DISCONNECTED_ERRORS as e:
                    raise MessageMiddlewareDisconnectedError(f"Se perdió la conexión al hacer ack en el exchange {self.exchange_name}") from e
                except pika.exceptions.AMQPError as e:
                    raise MessageMiddlewareMessageError(f"Error al hacer ack en el exchange {self.exchange_name}") from e

            def nack():
                try:
                    self.channel.basic_nack(
                        delivery_tag=method.delivery_tag
                    )
                except _DISCONNECTED_ERRORS as e:
                    raise MessageMiddlewareDisconnectedError(f"Se perdió la conexión al hacer nack en el exchange {self.exchange_name}") from e
                except pika.exceptions.AMQPError as e:
                    raise MessageMiddlewareMessageError(f"Error al hacer nack en el exchange {self.exchange_name}") from e

            on_message_callback(body, ack, nack)

        try:
            # ejecuta el callback cuando llega un mensaje
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=_rabbit_callback,
                auto_ack=False      # no lo confirma automaticamnete, sino que usa las funciones ack y nack
            )
            self.channel.start_consuming()
        except _DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(f"Se perdió la conexión mientras se consumía del exchange {self.exchange_name}") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error interno durante el consumo del exchange {self.exchange_name}") from e
    
    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except _DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(f"Se perdió la conexión al detener el consumo del exchange {self.exchange_name}") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error al detener el consumo del exchange {self.exchange_name}") from e
    
    def send(self, message):
        try:
            # asumo que usa la primer routing_key debido a que es el consumidor
            # en los tests aparece asi [routing_key], o sea un solo elemento
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=self.routing_keys[0],
                body=message
            )
        except _DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(f"Se perdió la conexión al enviar mensaje al exchange {self.exchange_name}") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error al enviar mensaje al exchange {self.exchange_name}") from e

    def close(self):
        try: 
            if self.channel.is_open:
                self.channel.close()
            if self.connection.is_open:
                self.connection.close()
        except _DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareCloseError("Error al cerrar la conexión") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError("Error al cerrar la conexión") from e