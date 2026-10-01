# Informe - TP Coordinación

## Estructura
Se utiliza Python como lenguaje principal. Son 5 main.py (cliente, gateway, sum, aggregation, join) más el middleware que se reutiliza del TP anterior.

De los elementos marcados en azul en el diagrama del README, el único modificable en el gateway es `message_handler`. El resto de los componentes modificables son `sum`, `aggregation`, `join` y el protocolo interno (`common/message_protocol/internal.py`).

## Arquitectura general
El flujo de un cliente es:
```
A modo de ejemplo para 1 cliente para el escenario 2.
  Cliente ---TCP--->   Gateway 
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
3. El middleware le entrega una copia a cada sum (incluido el emisor).
4. Cada sum, al recibir el aviso, verifica que no sea suyo (`sum_sender_id != self.sum_id`) y emite sus propios parciales a los aggregators.


En el sum hay dos threads
- `input_queue`: datos y EOF original (hilo principal).
- `SUM_CONTROL_EXCHANGE`: avisos de EOF de otros sums (hilo secundario).

##### Observacion
No hay orden garantizado entre los mensajes. El state_lock garantiza que dos threads no toquen el mismo diccionario al mismo tiempo. Eso evita que _process_data y _flush_client corrompan el estado. Pero no garantiza que todos los datos ya estén procesados cuando llega el aviso de control, se puede solucionar poniendo el prefetch_count=1 en el Middleware, de esa forma solo habrá 1 mensaje procesandose a la vez, reduciendo enormemente la posibilidad de una race condition. Lo dejé comentado en el codigo, en todo caso se descomenta.
En los tests funciona sin poner el prefetch=1 debido al volumen de los datasets y dado que los sums procesan rapido

### Coordinación de Aggregation
Cuando hay N sums y M aggregators:
- Cada sum emite sus parciales a un aggregator
- Cada sum emite su EOF a todos los aggregators.

Como cada sum manda su propio EOF, el aggregation recibe N EOFs por cliente (uno por cada sum). No puede emitir el top con el primero que llega, porque le faltarían los parciales de los otros sums. Para ello se hace un conteo de EOFs
Cuando el contador llega a `SUM_AMOUNT`, el aggregation ya tiene todos los parciales del cliente. Calcula los TOP_SIZE más grandes de su parte (shard) y los manda al join.


### Coordinación de Join
El join recibe M tops parciales por cliente (uno por cada aggregator). Cada aggregator calculó su top parcial sobre su shard de frutas.
El join mantiene, por cliente, una lista de parciales y un contador (partials_by_client).


### Manejo de SIGTERM
Los nodos sum, aggregation y join registran un handler para SIGTERM en main().
El handler pide frenar el consumo de cada conexión usando add_callback_threadsafe, que es la forma thread-safe que tiene pika para encolar una operación en el hilo dueño de la conexión.


### Escalabilidad
##### Escalado respecto a clientes
El gateway atiende a cada cliente en un proceso separado segun la cantidad de CPUs. Cada cliente tiene su propia instancia de MessageHandler con un client_id único que viaja en todos los mensajes internos.

En sum, aggregation y join, el estado se mantiene por cliente (amount_by_fruit_by_client[client_id], fruit_top_by_client[client_id], partials_by_client[client_id]). Eso permite procesar varios clientes concurrentemente sin que se mezclen sus acumulados.

##### Escalado respecto a volumen de datos
N sums reparten los mensajes de input_queue (working queue). A mayor cantidad de instancias de sums, menor mensajes dentro de cada uno

M aggregators se reparten las frutas por hash. A mayor cantidad de instancias de aggregators, menos frutas por aggregator

El estado en cada sum y aggregation se mantiene en memoria. La memoria de cada sum es proporcional a las frutas distintas por cliente activo, no a la cantidad de registros. Y entre sum y aggregation viajan a lo sumo (frutas distintas * sums) mensajes por cliente, sin importar cuántos registros haya

##### Escalado respecto a la cantidad de controles
Con más sums y más aggregators, el sistema escala aproximadamente de forma lineal:

Sums: RabbitMQ reparte round-robin. Cada sum procesa 1/N de los mensajes.

Aggregators: el hash `zlib.crc32(f"{client_id}:{fruit}") % M` distribuye las frutas entre los M aggregators. Como incluye el client_id, distribuye entre clientes también, evitando que todas las frutas de un cliente caigan en el mismo aggregator.

Join: espera M parciales por cliente. (M * TOP_SIZE ítems por cliente)

##### Sharding por hash
Se eligió hash con crc32 sobre client:fruta. No se usa hash de python porque es aleatorio por proceso

Para repartir las frutas entre los aggregators se usa:

> idx = zlib.crc32(f"{client_id}:{fruit}".encode()) % AGGREGATION_AMOUNT

Se incluye el client_id porque sino todas las apariciones de una fruta irían al mismo aggregator. Eso sesga la distribución si un cliente manda pocas frutas distintas. Con (client_id, fruit), el agregado se distribuye entre clientes.

Nota: como decia una consulta en el foro, el sistema es escalable, no elástico. Cambiar AGGREGATION_AMOUNT requiere reiniciar todos los procesos. No se soporta agregar o quitar nodos durante la ejecución.


### Limitaciones conocidas
El middleware (sin modificaciones respecto al TP anterior) crea una queue exclusiva cada vez que se instancia un MessageMiddlewareExchangeRabbitMQ, independientemente de si la instancia se usará para consumir o solo para publicar.
En el diseño actual del sum hay varias instancias que se usan solo para publicar:
- main_control_publisher: publica avisos de EOF al exchange de control.
- main_data_output_exchanges: publican los parciales a los aggregators.
- control_data_outputs: ídem para el hilo de control.
Cada una de esas instancias crea una queue bindeada al exchange correspondiente pero no son consumidos por nadie. Los mensajes que llegan a esas queues se acumulan en RabbitMQ hasta que el proceso termina.

Con los volúmenes del TP el impacto es despreciable. En una corrida prolongada con alto tráfico, esas queues podrían crecer y consumir memoria del broker. La solución limpia sería exponer un modo solo publicar en el middleware, que omita la creación de la queue. Si se quisiera, se agregaría un parámetro opcional al constructor (por ejemplo, create_queue=True/False)


