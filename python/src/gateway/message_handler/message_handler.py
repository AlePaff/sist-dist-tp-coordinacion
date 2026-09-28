import logging
from common import message_protocol


class MessageHandler:
    _next_client_id = 0
    # self.client_id = id(self)         # alternativa a un identificador unico


    def __init__(self):
        self.client_id = MessageHandler._next_client_id
        MessageHandler._next_client_id += 1
        logging.info(f"Cliente con ID {self.client_id}")
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id])

    # aqui se reciben los tops y se mandan a los clientes
    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        print(f"client {self.client_id} - fields {fields}")            # ejemplo fields [[0, [['nectarine', 3084], ['apple', 2736], ['banana', 2628]]]]
        # if not isinstance(fields, list) or len(fields) != 2:
        #     return None
        # si el cliente no coincide manda None para que el gateway siga buscando
        if fields[0] != self.client_id:
            return None

        return fields[1]        # devuelve el top
