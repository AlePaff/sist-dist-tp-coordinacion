import os
import logging
import socket
import signal
import multiprocessing
import message_handler
from common import middleware, message_protocol

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]


def handle_client_request(client_socket, message_handler):
    # aca se publican los mensajes que el cliente envíe, en la cola de rabbitMQ
    # es la cola "de salida" porque "salen" hacia el backend
    # el gateway escribe (output del gateway, input del backend)
    output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)

    try:
        while True:
            message = message_protocol.external.recv_msg(client_socket)

            # recibe una fruta y se la manda al message_handler
            if message[0] == message_protocol.external.MsgType.FRUIT_RECORD:
                serialized_message = message_handler.serialize_data_message(message[1])
                # se envia a la cola de salida de rabbitMQ
                output_queue.send(serialized_message)
                # se envia ack al cliente confirmando recepción
                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )

            if message[0] == message_protocol.external.MsgType.END_OF_RECODS:
                serialized_message = message_handler.serialize_eof_message(message[1])
                output_queue.send(serialized_message)
                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )
                return
    except socket.error:
        logging.error("The connection with the server was lost")
    except Exception as e:
        logging.error(e)
    finally:
        output_queue.close()


# consume respuestas del top y se las reenvia al cliente correspondiente
# o sea le manda el "top" de frutas
def handle_client_response(client_list):
    input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)      # se conecta a la cola de entrada
    # se llama de "entrada" porque vienen desde el servidor procesadas y se reenvian al cliente
    # "entran" al backend (input del gateway, output del backend)

    def _consume_result(message, ack, nack):
        client_index = 0
        try:
            for [message_handler_instance, client_socket] in client_list:
                deserialized_message = (
                    message_handler_instance.deserialize_result_message(message)
                )

                if not deserialized_message:
                    client_index += 1
                    continue

                message_protocol.external.send_msg(
                    client_socket,
                    message_protocol.external.MsgType.FRUIT_TOP,
                    deserialized_message,
                )
                message_protocol.external.recv_msg(client_socket)
                break
            client_list.pop(client_index)
            ack()
        except socket.error:
            logging.error("The connection with the server was lost")
            client_list.pop(client_index)
            ack()
        except Exception as e:
            logging.error(e)
            nack()
            input_queue.stop_consuming()

    input_queue.start_consuming(_consume_result)
    input_queue.close()


def handle_sigterm(server_socket, client_list, sigterm_received):
    server_socket.shutdown(socket.SHUT_RDWR)
    for [_, client_socket] in client_list:
        client_socket.shutdown(socket.SHUT_RDWR)
    sigterm_received.value = 1


def main():
    logging.basicConfig(level=logging.INFO)

    # manager permite crear estructuras (listas, diccionarios, etc) que pueden ser compartidas y modificadas entre procesos hijos
    with multiprocessing.Manager() as manager:
        client_list = manager.list()        # lista compartida entre procesos
        sigterm_received = manager.Value("c_short", 0)      # señal compartida entre procesos para el sigterm
        with multiprocessing.Pool(processes=os.process_cpu_count()) as processes_pool:      # pool de procesos con tantos workers como CPUs disponibles
            processes_pool.apply_async(handle_client_response, (client_list,))      # lanza la funcion asincrona (sin bloquear) handle_client_response en uno de los procesos

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                logging.info("Listening to connections")
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()      # se queda escuchando conexiones entrantes

                # crea la señal para un sigterm
                signal.signal(
                    signal.SIGTERM,
                    lambda signum, frame: handle_sigterm(
                        server_socket, client_list, sigterm_received
                    ),
                )
                while True:
                    try:
                        client_socket, _ = server_socket.accept()

                        logging.info("A new client has connected")
                        # crea una instancia para manejar a un cliente conectado
                        message_handler_instance = message_handler.MessageHandler()
                        # lo guarda en la lista de clientes
                        client_list.append([message_handler_instance, client_socket])
                        processes_pool.apply_async(
                            handle_client_request,
                            (client_socket, message_handler_instance),
                        )
                    except socket.error:
                        if sigterm_received.value == 0:
                            logging.error("The connection with the client was lost")
                            return 1
                        else:
                            return 0
                    except Exception as e:
                        logging.error(e)
                        return 2
    return 0


if __name__ == "__main__":
    main()
