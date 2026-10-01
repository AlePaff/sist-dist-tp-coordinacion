import os
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
        # obtiene la cola de entrada (la que viene del aggregation)
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        # obtiene la cola de salida (la que va al gateway)
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        # {client_id: [cantidad de parciales recibidos, [FruitItem, ...]]}
        #                     entry0                           entry1
        # ejemplo --> 0: [1, [FruitItem("manzana", 12), FruitItem("kiwi", 10)]]
        # 1 parcial del cliente 0 con esos dos FruitItem
        self.partials_by_client = {}


    def process_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 2:
            client_id, partial_top_tuple = fields

            logging.info(f"Received top from client {client_id}")
            # crea la entrada vacía si no existe, (0 parciales, lista vacía)
            entry = self.partials_by_client.setdefault(client_id, [0, []])
            entry[0] += 1           # contador de parciales
            entry[1].extend(fruit_item.FruitItem(f, a) for f, a in partial_top_tuple)
            logging.info(
                f"Partial top for client {client_id}: {entry[0]}/{AGGREGATION_AMOUNT}"
            )

            # todavía faltan aggregators: no emitir
            if entry[0] == AGGREGATION_AMOUNT:
                _, items = self.partials_by_client.pop(client_id)
                top = sorted(items, reverse=True)[:TOP_SIZE]
                self.output_queue.send(message_protocol.internal.serialize(
                    [client_id, [(i.fruit, i.amount) for i in top]]
                ))

        else:
            self.output_queue.send(message_protocol.internal.serialize(fields))
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
