# -*- coding: utf-8 -*-
"""
Tarea 3 - Big Data UNAD - Procesamiento en TIEMPO REAL
Consumidor (consumer) con Spark Structured Streaming: lee las lecturas de
temperatura del topic de Kafka 'temperatura_ideam', las limpia y calcula
estadísticas por ventanas de tiempo de 1 minuto, además de generar alertas.

Autor: Carlos Alberto Zuluaga Morales - Grupo 106

Ejecución:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
      spark_streaming_consumer_ideam.py
Detener: Ctrl + C
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, \
    TimestampType

TOPIC = "temperatura_ideam"
SERVIDOR = "localhost:9092"
TEMP_MIN_VALIDA, TEMP_MAX_VALIDA = -10.0, 45.0
UMBRAL_ALERTA = 35.0     # °C: se genera alerta de calor

spark = (SparkSession.builder
         .appName("Tarea3_Streaming_Temperatura_IDEAM")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1. Esquema del mensaje JSON que envía el productor
esquema = StructType([
    StructField("codigoestacion", StringType()),
    StructField("nombreestacion", StringType()),
    StructField("departamento", StringType()),
    StructField("municipio", StringType()),
    StructField("valorobservado", DoubleType()),
    StructField("unidadmedida", StringType()),
    StructField("fechaobservacion", TimestampType()),
])

# 2. Lectura en streaming desde el topic de Kafka
df_kafka = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", SERVIDOR)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "latest")
            .load())

# 3. Convertir el valor binario de Kafka (JSON) en columnas
lecturas = (df_kafka
            .select(F.from_json(F.col("value").cast("string"), esquema)
                    .alias("d"))
            .select("d.*")
            .withColumnRenamed("valorobservado", "temperatura"))

# 4. Limpieza en tiempo real: marcar lecturas válidas / erróneas
lecturas = lecturas.withColumn(
    "valida", F.col("temperatura").between(TEMP_MIN_VALIDA, TEMP_MAX_VALIDA))
validas = lecturas.filter(F.col("valida"))

# 5. Estadísticas por ventana de 1 minuto y departamento.
#    El watermark permite descartar datos que lleguen con más de 2 min de
#    retraso y liberar memoria de ventanas antiguas.
estadisticas = (validas
                .withWatermark("fechaobservacion", "2 minutes")
                .groupBy(F.window("fechaobservacion", "1 minute"),
                         "departamento")
                .agg(F.count("*").alias("lecturas"),
                     F.round(F.avg("temperatura"), 2).alias("promedio"),
                     F.min("temperatura").alias("minima"),
                     F.max("temperatura").alias("maxima"),
                     F.sum((F.col("temperatura") >= UMBRAL_ALERTA)
                           .cast("int")).alias("alertas_calor"))
                .select(F.date_format("window.start", "HH:mm:ss").alias("desde"),
                        F.date_format("window.end", "HH:mm:ss").alias("hasta"),
                        "departamento", "lecturas", "promedio", "minima",
                        "maxima", "alertas_calor"))

# 6. Alertas: lecturas de calor extremo o con falla de sensor
alertas = (lecturas
           .filter((~F.col("valida")) | (F.col("temperatura") >= UMBRAL_ALERTA))
           .withColumn("tipo_alerta",
                       F.when(~F.col("valida"), "FALLA DE SENSOR")
                        .otherwise("CALOR EXTREMO"))
           .select(F.date_format("fechaobservacion", "HH:mm:ss").alias("hora"),
                   "codigoestacion", "departamento", "municipio",
                   "temperatura", "tipo_alerta"))

# 7. Salida de resultados en consola (dos consultas en paralelo)
consulta_estadisticas = (estadisticas.writeStream
                         .queryName("estadisticas_por_minuto")
                         .outputMode("complete")
                         .format("console")
                         .option("truncate", "false")
                         .option("numRows", 50)
                         .trigger(processingTime="10 seconds")
                         .start())

consulta_alertas = (alertas.writeStream
                    .queryName("alertas")
                    .outputMode("append")
                    .format("console")
                    .option("truncate", "false")
                    .trigger(processingTime="10 seconds")
                    .start())

print("Consumidor iniciado. Esperando datos del topic", TOPIC, "...")
print("Interfaz web de Spark: http://IP_DE_LA_VM:4040")
spark.streams.awaitAnyTermination()
