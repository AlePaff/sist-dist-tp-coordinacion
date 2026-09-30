## Estructura
Se utilizará Python como lenguaje principal. Son 5 main.py

## 
Una vez copiado el TP MOM si hago "make test" pasaran los tests. 
Pero si hago "make switch" y selecciono 2, ahora se me modificará el docker-compose.yaml y cuando haga "make test" fallará

Los 5 escenarios se tienen que cumplir



De los listados en azul en el diagrama el unico que se puede modificar es el message_handler en el gateway. Despues todos los que estan en blanco.


## Interpretación "Limitaciones del esqueleto provisto"
 - No se implementa la interfaz del middleware. 
 -> el middleware es el tp anterior y ya fué implementado, es copiar y pegar.

 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
  -> esta es la parte a implementar y discriminar, por eso cuando pongo varios clientes rompe actualmente.

 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - No se puede escalar respecto a grandes volúmenes de datos transmitidos desde los clientes. 
   -> la idea es tambien escalar en la situación donde un cliente viene con un volumen muy grande de datos, y que el sistema pueda trabajar concurrentemente, ya sea un cliente con muchos datos o multiples clientes con datos

   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos. 
   -> ahora mismo no se sabe cuando se terminaron de procesar los datos, en un ambiente distribuido es muy dificil

   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante. 
   -> la idea es repartir el trabajo eficientemente

  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.
   -> hay que manejar esto


## Threading vs multiprocessing
threading en el tp0 eran para hilos dentro de un mismo proceso, eran afectados por el GIL, habia un costo "bajo" de crear threads.
En multiprocessing son procesos diferentes, no son afectados por el GIL cada proceso tiene su propio interprete, tienen un costo alto de creación.




## Arquitectura
1 cliente:
  cliente ---TPC--->   gateway 
    ----RabbitMQ(input_queue)---->    sum  
        ---RabbitMQ(por exchange a cada queue)---->   aggregation                               su output queue es "join_queue"
            ----RabbitMQ(output queue es 'join_queue')---->  join                           su input queue es "join_queue", su output queue es "results_queue"
              ----RabbitMQ----> gateway  ----> Cliente



## Comandos utiles
Para ver solo los logs de un servicio en particular, por ejemplo el gateway
> docker compose logs gateway


## Escenarios

### Escenario 3
Ahora que hay 3 sums repartidos entre multiples clientes si bien ya tengo una forma de identificar a quien pertenece cada mensaje de frutas (porque modifiqué para que incluya el client_id) tengo un problema. RabbitMQ al ver que hay 3 sums, o sea 3 colas, reparte mediante round robin, y ahora mismo el sum no tiene manera de saber cuando terminar, ahora mismo le enviará la finalización EOF a un solo sum (al ultimo que haya llegado) y los otros dos sums no tienen manera de enterarse, por eso se tiene que reenviar a los otros sums


##### Soluciones
###### Alternativa 1
Una alternativa es poner un emmited_clients en cada sum, de forma que cuando uno de ellos reciba un EOF se lo reenvie a otro sum, si bien en la practica funciona no es seguro, ya que podría pasar el improbable caso que sum_0 se reenvia a si mismo, y esa información del EOF nunca llega a los otros clientes, los cuales se quedan esperando. Aunque para este caso podría funcionar no es la solución mas limpia

###### Alternativa 2
La alternativa propuesta (y por la pista qu ehay en sum SUM_CONTROL_EXCHANGE) se espera que se utilice un exchange para saber cuando se terminó de procesar el archivo en los distintos sums
Aunque puede existir una race condition
Hay dos colas/canales por los que le llegan cosas al mismo sum:
input_queue (la cola del gateway): de acá llegan los mensajes de datos [client_id, fruit, amount] y el EOF original [client_id].
Exchange de control (SUM_CONTROL_EXCHANGE): de acá llegan los avisos de EOF de otros sums.
Ambos canales los consume el mismo sum, pero en hilos distintos. Y no hay orden garantizado entre canales. Eso es la race condition.

EJEMPLO
  Dentro de input_queue, RabbitMQ garantiza orden FIFO. Entonces:
    Si un cliente manda fruit_1, fruit_2, fruit_3, EOF, los mensajes entran a input_queue en ese orden.
    Entre canales no hay orden. Es decir:
    Que sum_0 reciba un aviso de EOF por el exchange de control no significa que sum_0 ya procesó todos los datos que le tocaron de ese cliente.
    Puede pasar que el aviso llegue mientras sum_0 está procesando un fruit_record de ese mismo cliente.


Cosas que surgieron.
En Pika BlockingConnection no es thread safe, por lo tanto no puedo tener un mismo exchange de conexión y que sea usado por dos threads distintos, por eso debo tomarlo como dos conexiones distintas, para este caso se utiliza el mismo exchange. 
NOTA: Tener en cuenta que la implementación de Middleware crea siempre una cola por conexión, y para el caso del exchange de control no es necesario, o sea se crea una cola pero nunca se hace start_consuming

 ¿A dónde van esos mensajes y qué objetivo cumplen?
Para entender a dónde van, hay que mirar el flujo completo de lo que pasa cuando un cliente termina de enviar datos:
El Hilo Principal de la instancia actual de SumFilter recibe un EOF de un cliente en su input_queue.
Vacía su acumulado: Manda lo que él acumuló de ese cliente hacia los AGGREGATION usando main_data_output_exchanges.
Avisa a sus pares: Llama a control_publisher.send(...). Este mensaje va al SUM_CONTROL_EXCHANGE de RabbitMQ.
RabbitMQ distribuye: El SUM_CONTROL_EXCHANGE actúa como un broadcast (o fanout/direct a los demás SumFilter). Le entrega una copia del mensaje a las colas de escucha de todas las demás instancias de SumFilter.   
Los otros SumFilter lo reciben: En los otros nodos, sus respectivos hilos secundarios (control_consumer) reciben la notificación en el método process_control_eof.
Los demás vacían su acumulado: Al recibir el aviso, las otras instancias envían el acumulado que ellas tenían guardado de ese mismo cliente hacia los aggregators mediante sus control_data_outputs.






##### Ejemplo
INFO:root:client_0
INFO:root:cranberry          693 - Expected: cranberry         1478
INFO:root:apple              630 - Expected: raspberry         1346
INFO:root:banana             538 - Expected: watermelon        1296
ERROR:root:Mistmatch in expected and received fruit tops

Estás corriendo el Escenario 3 con 3 sums pero sin el fix. Veamos qué pasa:

El gateway manda los datos del cliente 0 a input_queue. RabbitMQ los reparte round-robin entre sum_0, sum_1 y sum_2.

Cada sum acumula solo su parte. Por ejemplo, si cranberry tiene 1478 en total, cada sum puede haber visto ~493, y sum_0 (que recibe el EOF) emite solo su parcial: 693.

Los otros 2 sums nunca emiten porque nunca reciben el EOF.

El aggregation recibe solo los parciales de sum_0 y un solo EOF. Como SUM_AMOUNT=1 (mal configurado en el aggregation, o mejor dicho, el aggregation espera 1 EOF y lo recibe), emite el top con los datos incompletos.

El gateway le manda ese top incompleto al cliente 0.

output_0.csv tiene cranberry=693, apple=630, banana=538 → 1/3 de los datos reales, y las frutas que aparecen son las que sum_0 alcanzó a ver.

En resumen: el top está mal porque faltan los parciales de sum_1 y sum_2. Solo se contabilizó lo que vio sum_0.












