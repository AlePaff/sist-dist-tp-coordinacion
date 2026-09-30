import os
import logging
import bisect

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
        # se conecta al exchange AGGREGATION_PREFIX con las routing keys bindeadas para mandarselo a multiples queues
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        # ejemplo ---> {client_id: [FruitItem, ...]
        self.fruit_top_by_client = {}

        # recien cuando sé cuantos sums terminaron puedo hacer el join
        # ejemplo --> {client_0: 3, client_1: 1, ...}
        self.eof_count_by_client = {}


    def _process_data(self, client_id, fruit, amount): 
        # se procesa por cada mensaje que llega
        logging.info("Processing data message")
        fruit_top = self.fruit_top_by_client.setdefault(client_id, [])

        # itera todos los tops de frutas
        for i in range(len(fruit_top)):
            # si el mensaje recibido es una (fruta,cantidad) que ya existe
            if fruit_top[i].fruit == fruit:
                # para asegurarme que esté siempre ordenada: quito el elemento, lo sumo, luego lo añado en orden
                # Ejemplo: si tengo fruit_top=[apple:10, melon:30, banana:35] y debo agregar [apple:40]
                updated = fruit_top.pop(i) + fruit_item.FruitItem(fruit, amount)
                bisect.insort(fruit_top, updated)

                return
        # inserta el item fruit_top, usando la comparación de FruitItem. Bisect asume que la lista siempre está ordenada, sino falla
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        logging.info(f"Received EOF for client {client_id}")

        # solo continúa cuando recibió el EOF de todos los sums
        if not self._check_eof_by_client(client_id): 
            return

        # obtiene el top del cliente que termino (y lo saca del dict)
        fruit_top_list = self.fruit_top_by_client.pop(client_id, [])

        fruit_chunk = list(fruit_top_list[-TOP_SIZE:])      # lo corta según el TOP_SIZE (por default top 3)
        fruit_chunk.reverse()       # invierte el orden para que quede ordenada descendentemente
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        # manda el top al join conservando el identificador del cliente
        self.output_queue.send(message_protocol.internal.serialize([client_id, fruit_top]))

    def _check_eof_by_client(self, client_id):
        # incrementa el contador de EOFs de este cliente
        self.eof_count_by_client[client_id] = (
            self.eof_count_by_client.get(client_id, 0) + 1
        )
        logging.info(f"EOF count for client {client_id}: {self.eof_count_by_client[client_id]}/{SUM_AMOUNT}")

        # todavía no llegaron todos los parciales: no emitir
        if self.eof_count_by_client[client_id] < SUM_AMOUNT:
            return False

        # ya llegaron todos: limpiar el contador y emitir
        self.eof_count_by_client.pop(client_id, None)
        return True


    def process_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        logging.info(f"Process message with fields {fields}")
        #si recibe una fruta procesa la data, caso contrario se interpreta como EOF
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()






"""
Ejemplo de secuencia temporal real
Supongamos que el SumFilter manda esto (en orden):

text
1. ["manzana", 5]
2. ["naranja", 3]
3. ["manzana", 2]
4. ["pera", 7]
5. []              ← EOF
Mensaje 1: ["manzana", 5]
python
def _process_data(self, fruit, amount):
    for i in range(len(self.fruit_top)):   # len = 0 → no itera
        ...
    bisect.insort(self.fruit_top, FruitItem("manzana", 5))
self.fruit_top está vacía → el for no ejecuta nada.

bisect.insort inserta FruitItem("manzana", 5).

Ahora self.fruit_top = [FruitItem("manzana", 5)].

Acá tenés razón: en el primer mensaje, el for no hace nada. Pero eso es solo el primer mensaje.

Mensaje 2: ["naranja", 3]
python
def _process_data(self, fruit, amount):
    for i in range(len(self.fruit_top)):   # len = 1 → itera i=0
        if self.fruit_top[0].fruit == "naranja":  # "manzana" == "naranja" → False
            ...
    bisect.insort(self.fruit_top, FruitItem("naranja", 3))
Ahora len(self.fruit_top) == 1 → el for sí itera una vez (i=0).

Compara "manzana" == "naranja" → False → no entra al if.

bisect.insort inserta FruitItem("naranja", 3).

Ahora self.fruit_top = [FruitItem("manzana", 5), FruitItem("naranja", 3)] (ordenado por lo que defina FruitItem).

Mensaje 3: ["manzana", 2]
python
def _process_data(self, fruit, amount):
    for i in range(len(self.fruit_top)):   # len = 2 → itera i=0, i=1
        if self.fruit_top[0].fruit == "manzana":   # True en i=0
            self.fruit_top[0] = self.fruit_top[0] + FruitItem("manzana", 2)
            return
len == 2 → itera.

En i=0, self.fruit_top[0].fruit == "manzana" → True.

Suma: FruitItem("manzana", 5) + FruitItem("manzana", 2) = FruitItem("manzana", 7).

Reemplaza self.fruit_top[0] y retorna (no inserta de nuevo).

Acá el for es esencial: evita duplicar la fruta y acumula el amount.
"""