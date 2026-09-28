import os
import logging
import threading

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
    def __init__(self):
        # viene del gateway la queue
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        # crea exchanges (es la oficina de correos, mensajes para ser despachados a las distintas queues)
        for i in range(AGGREGATION_AMOUNT):
            # pone las routing keys
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        # acumula el total por fruta
        self.amount_by_fruit_by_client = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data: client_id:{client_id} - {fruit},{amount}")
        amount_by_fruit = self.amount_by_fruit_by_client.setdefault(client_id, {})      # pone las frutas del cliente si ya existe, si no existe crea uno vacío
        # recibe una fruta y una cantidad y va sumando.
        # suma al acumulado de una fruta, o un fruitItem con amount 0 si no existe
        amount_by_fruit[fruit] = amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting data messages for client {client_id}")
        # obtiene el acumulado del cliente que termino (y lo saca del dict)
        amount_by_fruit = self.amount_by_fruit_by_client.pop(client_id, {})

        # al recibir EOF emite a cada exchange el total acumulado por cada fruta (ej. banana 3, manzana 7, etc. a cada aggregation)
        # de un cliente en particular
        for final_fruit_item in amount_by_fruit.values():
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )

        logging.info(f"Broadcasting EOF message for client {client_id}")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))


    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        # si es (fruit, amount) entonces acumula
        if len(fields) == 3:
            self._process_data(*fields)
        # en cualquier otro caso se interpreta como EOF
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        # inicia el consumo de la cola de entrada
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
