import os
import logging
import signal
import threading
import zlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:

    def _build_data_output_exchanges(self):
        # crea exchanges, donde los mensajes son despachados a las distintas queues
        return [
            # pone las routing keys
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            for i in range(AGGREGATION_AMOUNT)
        ]

    def _build_control_exchange(self):
        # exchange de control (direct: enviar a X routing keys) para sincronizar EOFs entre instancias de SUM
        return middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, ["eof_routing_key"]
        )


    def __init__(self):
        self.sum_id = ID
         # acumula el total por fruta
        self.amount_by_fruit_by_client = {}
        self.state_lock = threading.Lock()

        # -- recursos hilo principal (main) --
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        # NOTE: cambio sencillo para garantizar (casi al 100%) orden de mensajes
        # self.input_queue.channel.basic_qos(prefetch_count=1)

        self.main_data_output_exchanges = self._build_data_output_exchanges()
        self.main_control_publisher = self._build_control_exchange()  # solo send(), usado por hilo principal

        # -- recursos del hilo de control (control) --
        self.control_consumer = self._build_control_exchange()   # solo start_consuming(), usado por hilo secundario
        self.control_data_outputs = self._build_data_output_exchanges()


        


    def _aggregator_for(self, client_id, fruit):
        # ejemplo: zlib.crc32("3:banana".encode()) --> 3493508999 % 3 ---> 2
        return zlib.crc32(f"{client_id}:{fruit}".encode()) % AGGREGATION_AMOUNT


    def _process_data(self, client_id, fruit, amount):
        # se van sumando las frutas y se guardan localmente
        with self.state_lock:
            logging.info(f"Process data: client_id:{client_id} - {fruit},{amount}")
            amount_by_fruit = self.amount_by_fruit_by_client.setdefault(client_id, {})      # pone las frutas del cliente si ya existe, si no existe crea uno vacío
            # recibe una fruta y una cantidad y va sumando.
            # suma al acumulado de una fruta, o un fruitItem con amount 0 si no existe
            amount_by_fruit[fruit] = amount_by_fruit.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))


    def _flush_client(self, client_id, data_outputs):
        """Envía el acumulado + EOF a los aggregators usando las conexiones
        que le pasan (las del hilo que la llama). NOTE: Aca no se avisa a otros SUM."""

        logging.debug(f"Broadcasting data messages for client {client_id}")
        # obtiene el acumulado del cliente que termino (y lo saca del dict)
        with self.state_lock:
            amount_by_fruit = self.amount_by_fruit_by_client.pop(client_id, {})

        # al recibir EOF emite a cada exchange el total acumulado por cada fruta (ej. banana 3, manzana 7, etc. a cada aggregation)
        # de un cliente en particular, hasheado para poder distribuir equitativamente entre los aggregators
        for final_fruit_item in amount_by_fruit.values():
            idx = self._aggregator_for(client_id, final_fruit_item.fruit)
            # en base al indice del aggregator manda los sums
            data_outputs[idx].send(message_protocol.internal.serialize(
                [client_id, final_fruit_item.fruit, final_fruit_item.amount]
            ))

        logging.info(f"Broadcasting EOF message for client {client_id}")
        for out_exchange in data_outputs:
            out_exchange.send(message_protocol.internal.serialize([client_id]))

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        # si es (fruit, amount) entonces acumula
        if len(fields) == 3:
            self._process_data(*fields)
        # en cualquier otro caso se interpreta como EOF
        else:
            client_id = fields[0]
            logging.info(f"Recibido un EOF de parte del client:{client_id}")
            # envía el acumulado y le avisa a los aggregators del EOF
            self._flush_client(client_id, self.main_data_output_exchanges)
            # avisar a los demas SUM (solo acá en el hilo principal, no en el hilo de control)
            self.main_control_publisher.send(
                message_protocol.internal.serialize([client_id, self.sum_id])
            )
        ack()

    

    # callback del hilo secundario - se queda escuchando a los mensajes EOF que vienen del exchange
    def process_control_eof(self, message, ack, nack):
        client_id, sum_sender_id = message_protocol.internal.deserialize(message)

        # para no enviarse a si mismo
        if sum_sender_id != self.sum_id:
            logging.info(f"process_control_eof: recibido un EOF de sum:{sum_sender_id} con client:{client_id}")
            # self._process_eof(client_id)
            self._flush_client(client_id, self.control_data_outputs)

        
        ack()


    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.input_queue.connection.add_callback_threadsafe(
            self.input_queue.stop_consuming
        )
        self.control_consumer.connection.add_callback_threadsafe(
            self.control_consumer.stop_consuming
        )

    def start(self):
        # en un hilo aparte, se espera recibir mensajes del exchange de control
        control_thread = threading.Thread(
                target=self.control_consumer.start_consuming,
                args=(self.process_control_eof,)
            )
        control_thread.start()

        try:
            # inicia el consumo de la cola de entrada
            self.input_queue.start_consuming(self.process_data_messsage)
        finally:
            # el hilo principal salió (SIGTERM o error): esperar al de control
            control_thread.join()

            # cerrar todo, cada recurso en su hilo correspondiente
            self.input_queue.close()
            self.control_consumer.close()
            self.main_control_publisher.close()
            for e in self.main_data_output_exchanges:
                e.close()
            for e in self.control_data_outputs:
                e.close()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    signal.signal(signal.SIGTERM, sum_filter.handle_sigterm)
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
