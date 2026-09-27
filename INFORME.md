## Estructura
Se utilizará Python como lenguaje principal. Son 5 main.py

## 
Una vez copiado el TP MOM si hago "make test" pasaran los tests. 
Pero si hago "make switch" y selecciono 2, ahora se me modificará el docker-compose.yaml y cuando haga "make test" fallará

Los 5 escenarios se tienen que cumplir



De los listados en azul en el diagrama el unico que se puede modificar es el message_handler en el gateway. Despues todos los que estan en blanco.

## Interpretación "Limitaciones del esqueleto provisto"
 - No se implementa la interfaz del middleware. -> el middleware es el tp anterior y ya fué implementado, es copiar y pegar.
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente. -> esta es la parte a implementar y discriminar, por eso cuando pongo varios clientes rompe actualmente.
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - No se puede escalar respecto a grandes volúmenes de datos transmitidos desde los clientes. -> la idea es tambien escalar en la situación donde un cliente viene con un volumen muy grande de datos, y que el sistema pueda trabajar concurrentemente, ya sea un cliente con muchos datos o multiples clientes con datos
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos. -> ahora mismo no se sabe cuando se terminaron de procesar los datos, en un ambiente distribuido es muy dificil
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante. -> la idea es repartir el trabajo eficientemente
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway. -> hay que manejar esto
