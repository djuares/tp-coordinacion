# Coordinación de Sum y Aggregation

La solución separa los datos por `client_id`, de modo que varias consultas pueden
procesarse concurrentemente sin mezclar sus resultados. El `MessageHandler` del
Gateway asigna un identificador único a cada conexión y lo incorpora a todos los
mensajes internos.

## Coordinación de las instancias de Sum

Todas las instancias de `Sum` consumen la misma cola de entrada de RabbitMQ. El
middleware utiliza `prefetch_count=1`, por lo que los mensajes se distribuyen entre
las réplicas y cada instancia procesa una parte del flujo.

El mensaje `EOF` se publica mediante un exchange de control con una cola exclusiva
por instancia de `Sum`. De esta forma, todas las réplicas reciben la notificación
de finalización y pueden volcar el estado que procesaron para ese cliente.

El estado de `Sum` se mantiene por cliente y por fruta. No se almacenan los
registros originales: para cada cliente se conserva solamente la suma acumulada
por fruta. Una vez enviado el resultado parcial, el estado del cliente se elimina.

## Distribución entre Aggregation

Cada fruta se asigna determinísticamente a una única instancia de `Aggregation`:

```text
aggregation_id = CRC32(fruit) % AGGREGATION_AMOUNT
```

Por lo tanto, una fruta no se envía a todas las Aggregation. Cada `Sum` agrupa sus
resultados por partición y sólo envía datos a las Aggregation que tienen elementos
para esa partición.

Además, se reutiliza un único exchange productor por instancia de `Sum` y se
selecciona el routing key correspondiente mediante `send_to`. Esto evita crear una
conexión RabbitMQ independiente por cada combinación `Sum × Aggregation`.

Cada `Sum` envía después un mensaje `sum_done` a cada `Aggregation`. El mensaje se
publica por el mismo exchange y routing key utilizados para los datos de esa
Aggregation, después de los datos del `Sum`. La `Aggregation` mantiene, por cliente,
el conjunto de `sum_id` que ya finalizaron y calcula el top sólo cuando recibió la
finalización de todas las instancias de `Sum`.

Así se evita usar mensajes de datos vacíos como mecanismo de coordinación.

## Coordinación de Aggregation y Join

Cada `Aggregation` calcula un top parcial y agrega su propio `aggregation_id` al
resultado. `Join` acumula los tops por cliente y espera una respuesta de cada
`Aggregation` configurada. El conjunto de `aggregation_id` evita contar dos veces
una misma respuesta.

Cuando están disponibles todas las Aggregation, `Join` calcula el top final y lo
envía al Gateway junto con el `client_id`, permitiendo que el Gateway lo entregue al
cliente correspondiente.

## Escalabilidad

La solución aprovecha todas las réplicas configuradas de `Sum` porque comparten la
cola de entrada, y todas las réplicas de `Aggregation` porque cada una posee una
partición de frutas.

Respecto del volumen de datos, `Sum` mantiene sólo los acumulados por fruta y
cliente, en lugar de conservar todos los registros de entrada. Los estados se
eliminan al finalizar cada cliente.

Respecto de la cantidad de controles, los datos no se transmiten por broadcast a
todas las Aggregation: una fruta llega solamente a la partición que le corresponde.
También se evita generar un mensaje de datos vacío para cada combinación `Sum ×
Aggregation`; sólo se generan mensajes de datos para particiones que contienen
información y mensajes pequeños de finalización para coordinar el cierre.
