# Informe - TP Coordinación

## Estructura
Se utilizará Python como lenguaje principal. Son 5 main.py (cliente, gateway, sum, aggregation, join) más el middleware que se reutiliza del TP anterior.

De los elementos marcados en azul en el diagrama del README, el único modificable en el gateway es `message_handler`. El resto de los componentes modificables son `sum`, `aggregation`, `join` y el protocolo interno (`common/message_protocol/internal.py`).

## Arquitectura general
El flujo de un cliente es:
```
Para 1 cliente:
  Cliente ---TPC--->   Gateway 
    ----RabbitMQ(input_queue)---->  Sum  
        ---RabbitMQ(por exchange a cada queue)---->   Aggregation                       su output queue es "join_queue"
            ----RabbitMQ(output queue es 'join_queue')---->  Join                       su input queue es "join_queue", su output queue es "results_queue"
              ----RabbitMQ----> Gateway  ----> Cliente
```

- **Cliente**: lee un CSV, manda pares `(fruta, cantidad)` por TCP y espera el top final.
- **Gateway**: punto de entrada y salida. Traduce entre el protocolo externo (TCP) y el interno (RabbitMQ).
- **Sum**: acumula por fruta y por cliente. Al recibir el EOF del cliente, emite sus parciales a los aggregators.
- **Aggregation**: consolida los parciales de los sums. Cuando están todos, calcula un top parcial y lo manda al join.
- **Join**: combina los tops parciales de los aggregators y emite el top final al gateway.


### Comandos utiles
Para ver solo los logs de un servicio en particular, por ejemplo el gateway
> docker compose logs gateway

### Coordinación de Sum
Cuando hay N sums, RabbitMQ reparte los mensajes de `input_queue` entre ellos (working queue). Cada sum acumula solo los datos que le tocaron. El problema es que el EOF del cliente lo recibe _un solo sum_, y los demás no se enteran de que el cliente terminó.

#### Solución: exchange de control
Para mantener el comportamiento del Middleware se utiliza un exchange dedicado, `SUM_CONTROL_EXCHANGE`, de tipo direct con routing key `eof_routing_key`. Todos los sums se bindean a esa misma routing key, por lo que un mensaje publicado con esa key llega a todos (broadcast).

El flujo es:
1. El sum que recibe el EOF original por `input_queue`, emite sus parciales a los aggregators.
2. Ese mismo sum publica `[client_id, sum_id]` en `SUM_CONTROL_EXCHANGE`.
3. RabbitMQ le entrega una copia a cada sum (incluido el emisor).
4. Cada sum, al recibir el aviso, verifica que no sea suyo (`sum_sender_id != self.sum_id`) y emite sus propios parciales a los aggregators.
5. Cada sum manda su EOF al aggregation.

En el sum hay dos threads
- `input_queue`: datos y EOF original (hilo principal).
- `SUM_CONTROL_EXCHANGE`: avisos de EOF de otros sums (hilo secundario).

##### Observacion
No hay orden garantizado entre los mensajes. El state_lock garantiza que dos threads no toquen el mismo diccionario al mismo tiempo. Eso evita que _process_data y _flush_client corrompan el estado. Pero no garantiza que todos los datos ya estén procesados cuando llega el aviso de control, se puede solucionar poniendo el prefetch=1 en el Middleware, de esa forma solo habrá 1 mensaje procesandose a la vez, reduciendo enormemente la posibilidad de una race condition. 
En los tests funciona sin poner el prefetch=1 debido al volumen de los datasets y dado que los sums procesan rapido

### Coordinación de Aggregation
Cuando hay N sums y M aggregators:
- Cada sum emite sus parciales a un aggregator (elegido por hash)
- Cada sum emite su EOF a todos los aggregators.

Como cada sum manda su propio EOF, el aggregation recibe N EOFs por cliente (uno por cada sum). No puede emitir el top con el primero que llega, porque le faltarían los parciales de los otros sums. Para ello se hace un conteo de EOFs
Cuando el contador llega a `SUM_AMOUNT`, el aggregation ya tiene todos los parciales del cliente. Calcula los TOP_SIZE más grandes de su parte (shard) y los manda al join.


### Coordinación de Join
El join recibe M tops parciales por cliente (uno por cada aggregator). Cada aggregator calculó su top parcial sobre su shard de frutas.
El join mantiene, por cliente, una lista de parciales y un contador (partials_by_client).


### Manejo de SIGTERM
Los nodos sum, aggregation y join registran un handler para SIGTERM en main().
> signal.signal(signal.SIGTERM, sum_filter.handle_sigterm)

El handler pide frenar el consumo de cada conexión usando add_callback_threadsafe, que es la forma thread-safe que tiene pika para encolar una operación en el hilo dueño de la conexión.

