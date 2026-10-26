# Tarea 3 – Procesamiento de datos con Apache Spark
**Análisis de la temperatura del aire en Colombia (red de estaciones IDEAM)**

Curso Big Data (202016911) – UNAD – Grupo 106
Autor: Carlos Alberto Zuluaga Morales

---

## 1. Problema

Colombia tiene una red de cientos de estaciones automáticas del IDEAM que miden la temperatura del aire varias veces por hora, así que se acumulan millones de registros. Con datos de ese tamaño ya no sirve una hoja de cálculo. Este proyecto usa **Apache Spark** para:

1. Limpiar los datos y separar las lecturas erróneas de los sensores (por ejemplo, valores de 99.9 °C o −99 °C).
2. Encontrar patrones de temperatura por **departamento, hora del día y mes**.
3. Encontrar los municipios con más episodios de **calor extremo (≥ 35 °C)**.
4. Detectar **lecturas anómalas** por estación con el puntaje z.
5. Simular la llegada de lecturas en **tiempo real** con **Kafka** y procesarlas con **Spark Structured Streaming**, con estadísticas por minuto y alertas.

## 2. Conjunto de datos

| Campo | Detalle |
|---|---|
| Nombre | Temperatura Ambiente del Aire – Datos hidrometeorológicos crudos, red de estaciones IDEAM |
| Fuente | Portal de Datos Abiertos de Colombia – <https://www.datos.gov.co/d/sbwg-7ju4> |
| Formato | CSV (API Socrata) |
| Subconjunto usado | Año 2024 (todas las estaciones del país) |
| Columnas | `codigoestacion`, `codigosensor`, `fechaobservacion`, `valorobservado`, `nombreestacion`, `departamento`, `municipio`, `zonahidrografica`, `latitud`, `longitud`, `descripcionsensor`, `unidadmedida` |

## 3. Estructura del repositorio

```
├── tarea3_batch_ideam.py              # Procesamiento en batch (DataFrames, Spark SQL y RDDs)
├── kafka_producer_ideam.py            # Productor Kafka: simula estaciones en tiempo real
├── spark_streaming_consumer_ideam.py  # Consumidor Spark Structured Streaming
├── requirements.txt
├── comandos_vm.txt                    # Paso a paso completo de comandos en la VM
└── graficas/                          # Gráficas generadas por el análisis batch
```

## 4. Requisitos

- Máquina virtual Ubuntu con Hadoop 3 y Spark 3.5 configurados (Anexos 1 y 2 de la Tarea 2)
- Python 3 y las librerías de `requirements.txt`:
  ```bash
  pip install -r requirements.txt
  ```
- Apache Kafka 3.9 (Anexo 3), solo para la parte de tiempo real

## 5. Ejecución – Procesamiento en batch

```bash
# 1) Descargar el dataset desde la fuente original (año 2024)
wget -O temperatura_ideam.csv "https://www.datos.gov.co/resource/sbwg-7ju4.csv?\$where=fechaobservacion%20between%20'2024-01-01T00:00:00'%20and%20'2024-12-31T23:59:59'&\$limit=1000000"

# 2) Copiarlo a HDFS (con Hadoop iniciado: start-all.sh)
hdfs dfs -mkdir -p /Tarea3
hdfs dfs -put -f temperatura_ideam.csv /Tarea3/

# 3) Ejecutar el análisis
python3 tarea3_batch_ideam.py
# o indicando la ruta de entrada y la de salida:
python3 tarea3_batch_ideam.py hdfs://localhost:9000/Tarea3/temperatura_ideam.csv hdfs://localhost:9000/Tarea3/resultados
```

**¿Qué hace el script?**

| Etapa | Operaciones Spark |
|---|---|
| Carga | `spark.read.csv` con esquema explícito (más rápido que `inferSchema`) |
| Limpieza | conteo de nulos, `to_timestamp`, `trim/upper`, `dropna`, `dropDuplicates`, filtro de rango físico (−10 °C a 45 °C) |
| Transformación | columnas derivadas: año, mes, hora, día, franja horaria; `cache()` |
| EDA | `summary()`, `groupBy/agg` por departamento, hora, mes y franja; pisos térmicos por estación; top de estaciones; calor extremo |
| Anomalías | Funciones de ventana (`Window.partitionBy`) para calcular el puntaje z por estación |
| Spark SQL | Amplitud térmica diaria promedio por departamento |
| RDDs | `map`, `filter`, `reduceByKey`, `mapValues`, `sortBy`, `take`, `count`, `reduce`, `histogram` |
| Salida | Resultados en CSV en HDFS y 4 gráficas PNG en `./graficas/` |

## 6. Ejecución – Procesamiento en tiempo real (Kafka + Spark Streaming)

```bash
# Terminal 1: iniciar ZooKeeper y Kafka
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &

# Crear el topic (3 particiones)
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --replication-factor 1 --partitions 3 --topic temperatura_ideam

# Terminal 1: productor
python3 kafka_producer_ideam.py

# Terminal 2: consumidor
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer_ideam.py
```

- El **productor** envía una lectura por segundo de 8 estaciones simuladas, con los mismos campos del dataset del IDEAM. Usa el código de la estación como *key*, así que cada estación siempre va a la misma partición. Alrededor del 3 % de las lecturas simulan una falla de sensor y el 5 % un pico de calor.
- El **consumidor** lee el topic, convierte el JSON en columnas y descarta las lecturas inválidas. Con ventanas de 1 minuto y un *watermark* de 2 minutos calcula, por departamento, el número de lecturas, el promedio, la mínima, la máxima y las alertas de calor. Una segunda consulta muestra en vivo las alertas de **CALOR EXTREMO** y de **FALLA DE SENSOR**.
- Los resultados se ven en la consola cada 10 segundos y en la interfaz web de Spark: `http://IP_DE_LA_VM:4040`.

## 7. Referencias

- IDEAM. (2024). *Temperatura ambiente del aire* [Conjunto de datos]. Datos Abiertos Colombia. https://www.datos.gov.co/d/sbwg-7ju4
- Apache Software Foundation. (2024). *Structured Streaming programming guide*. https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
