# -*- coding: utf-8 -*-
"""
Tarea 3 - Procesamiento de Datos con Apache Spark (Big Data - UNAD)
Procesamiento en BATCH del conjunto de datos:
    "Temperatura Ambiente del Aire" - Red de estaciones del IDEAM
    Fuente: https://www.datos.gov.co/d/sbwg-7ju4  (Datos Abiertos Colombia)

Problema:
    Analizar el comportamiento de la temperatura del aire registrada por las
    estaciones automáticas del IDEAM para identificar patrones por departamento,
    hora del día y mes, detectar temperaturas extremas y lecturas anómalas
    (posibles fallas de sensores) que afectan la calidad de los datos.

Autor: Carlos Alberto Zuluaga Morales - Grupo 106

Ejecución (en la máquina virtual, usuario vboxuser):
    python3 tarea3_batch_ideam.py
    python3 tarea3_batch_ideam.py hdfs://localhost:9000/Tarea3/temperatura_ideam.csv
"""

import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType)

# ---------------------------------------------------------------------------
# 0. Configuración
# ---------------------------------------------------------------------------
RUTA_ENTRADA = (sys.argv[1] if len(sys.argv) > 1
                else "hdfs://localhost:9000/Tarea3/temperatura_ideam.csv")
RUTA_SALIDA = (sys.argv[2] if len(sys.argv) > 2
               else "hdfs://localhost:9000/Tarea3/resultados")
CARPETA_GRAFICAS = "graficas"

# Rango físicamente plausible para temperatura del aire en Colombia (°C).
# Valores fuera de este rango se consideran errores de sensor.
TEMP_MIN_VALIDA = -10.0
TEMP_MAX_VALIDA = 45.0
UMBRAL_CALOR = 35.0        # °C, temperatura extrema alta
UMBRAL_Z = 3.0             # desviaciones estándar para marcar anomalías


def titulo(texto):
    """Imprime un encabezado para que la salida en consola sea fácil de leer."""
    print("\n" + "=" * 78)
    print(" " + texto)
    print("=" * 78)


# ---------------------------------------------------------------------------
# 1. Sesión de Spark
# ---------------------------------------------------------------------------
spark = (SparkSession.builder
         .appName("Tarea3_Batch_Temperatura_IDEAM")
         # Menos particiones de shuffle: la VM tiene pocos núcleos y el valor
         # por defecto (200) crea demasiadas tareas pequeñas.
         .config("spark.sql.shuffle.partitions", "8")
         # Oculta la barra de progreso para que la salida sea más legible
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 2. Carga de datos desde HDFS (fuente original: datos.gov.co)
#    Se define el esquema explícito: es más rápido que inferSchema en
#    archivos grandes y evita que Spark adivine mal los tipos.
# ---------------------------------------------------------------------------
esquema = StructType([
    StructField("codigoestacion", StringType()),
    StructField("codigosensor", StringType()),
    StructField("fechaobservacion", StringType()),
    StructField("valorobservado", DoubleType()),
    StructField("nombreestacion", StringType()),
    StructField("departamento", StringType()),
    StructField("municipio", StringType()),
    StructField("zonahidrografica", StringType()),
    StructField("latitud", DoubleType()),
    StructField("longitud", DoubleType()),
    StructField("descripcionsensor", StringType()),
    StructField("unidadmedida", StringType()),
])

titulo("1. CARGA DE DATOS")
df_crudo = (spark.read
            .option("header", "true")
            .option("encoding", "UTF-8")
            .schema(esquema)
            .csv(RUTA_ENTRADA))

total_crudo = df_crudo.count()
print(f"Archivo: {RUTA_ENTRADA}")
print(f"Registros cargados: {total_crudo:,}")
print(f"Particiones del DataFrame: {df_crudo.rdd.getNumPartitions()}")
df_crudo.printSchema()
df_crudo.show(5, truncate=False)

# ---------------------------------------------------------------------------
# 3. Limpieza de datos
# ---------------------------------------------------------------------------
titulo("2. LIMPIEZA DE DATOS")

# 3.1 Valores nulos por columna (antes de limpiar)
print("Valores nulos por columna:")
df_crudo.select([F.sum(F.col(c).isNull().cast("int")).alias(c)
                 for c in df_crudo.columns]).show(truncate=False)

df = (df_crudo
      # 3.2 Convertir la fecha (formato 2024-01-31T13:00:00.000) a timestamp
      .withColumn("fecha", F.to_timestamp(
          F.regexp_replace("fechaobservacion", "T", " ")))
      # 3.3 Normalizar textos: quitar espacios sobrantes y pasar a mayúsculas
      .withColumn("departamento", F.upper(F.trim("departamento")))
      .withColumn("municipio", F.upper(F.trim("municipio")))
      .withColumn("nombreestacion",
                  F.regexp_replace(F.trim("nombreestacion"), r"\s+", " "))
      .withColumnRenamed("valorobservado", "temperatura")
      # 3.4 Eliminar registros sin datos esenciales
      .dropna(subset=["codigoestacion", "fecha", "temperatura",
                      "departamento"])
      # 3.5 Eliminar duplicados (misma estación, misma fecha y hora)
      .dropDuplicates(["codigoestacion", "fecha"]))

total_sin_nulos = df.count()

# 3.6 Separar lecturas fuera de rango físico (errores de sensor)
cond_valida = F.col("temperatura").between(TEMP_MIN_VALIDA, TEMP_MAX_VALIDA)
df_invalidos = df.filter(~cond_valida)
df = df.filter(cond_valida)

total_limpio = df.count()
print(f"Registros iniciales ............................ {total_crudo:,}")
print(f"Eliminados por nulos o duplicados .............. "
      f"{total_crudo - total_sin_nulos:,}")
print(f"Eliminados por estar fuera de [{TEMP_MIN_VALIDA}, {TEMP_MAX_VALIDA}] °C ... "
      f"{total_sin_nulos - total_limpio:,}")
print(f"Registros limpios para el análisis ............. {total_limpio:,}")

# ---------------------------------------------------------------------------
# 4. Transformación: nuevas columnas derivadas de la fecha y la temperatura
# ---------------------------------------------------------------------------
titulo("3. TRANSFORMACIÓN")
df = (df
      .withColumn("anio", F.year("fecha"))
      .withColumn("mes", F.month("fecha"))
      .withColumn("hora", F.hour("fecha"))
      .withColumn("dia", F.to_date("fecha"))
      .withColumn("franja_horaria",
                  F.when(F.col("hora") < 6, "1-Madrugada")
                   .when(F.col("hora") < 12, "2-Mañana")
                   .when(F.col("hora") < 18, "3-Tarde")
                   .otherwise("4-Noche"))
      .select("codigoestacion", "nombreestacion", "departamento",
              "municipio", "zonahidrografica", "latitud", "longitud",
              "fecha", "dia", "anio", "mes", "hora", "franja_horaria",
              "temperatura"))

# El DataFrame se reutiliza muchas veces: se guarda en memoria (cache)
df.cache()
df.show(5, truncate=False)

# ---------------------------------------------------------------------------
# 5. Análisis exploratorio (EDA) con DataFrames
# ---------------------------------------------------------------------------
titulo("4. ANÁLISIS EXPLORATORIO DE DATOS (EDA)")

print("4.1 Estadísticas descriptivas de la temperatura (°C):")
df.select("temperatura").summary(
    "count", "mean", "stddev", "min", "25%", "50%", "75%", "max").show()

rango = df.agg(F.min("fecha").alias("desde"), F.max("fecha").alias("hasta"),
               F.countDistinct("codigoestacion").alias("estaciones"),
               F.countDistinct("departamento").alias("departamentos"),
               F.countDistinct("municipio").alias("municipios")).first()
print(f"Periodo analizado: {rango['desde']}  a  {rango['hasta']}")
print(f"Estaciones: {rango['estaciones']} | Departamentos: "
      f"{rango['departamentos']} | Municipios: {rango['municipios']}")

print("\n4.2 Temperatura por departamento (ordenado por promedio):")
por_depto = (df.groupBy("departamento")
             .agg(F.count("*").alias("lecturas"),
                  F.countDistinct("codigoestacion").alias("estaciones"),
                  F.round(F.avg("temperatura"), 2).alias("promedio"),
                  F.round(F.min("temperatura"), 2).alias("minima"),
                  F.round(F.max("temperatura"), 2).alias("maxima"),
                  F.round(F.stddev("temperatura"), 2).alias("desv_std"))
             .orderBy(F.desc("promedio")))
por_depto.show(40, truncate=False)

print("4.3 Ciclo diario: temperatura promedio por hora del día:")
por_hora = (df.groupBy("hora")
            .agg(F.round(F.avg("temperatura"), 2).alias("promedio"),
                 F.count("*").alias("lecturas"))
            .orderBy("hora"))
por_hora.show(24)

print("4.4 Temperatura promedio por mes:")
por_mes = (df.groupBy("anio", "mes")
           .agg(F.round(F.avg("temperatura"), 2).alias("promedio"),
                F.round(F.max("temperatura"), 2).alias("maxima"),
                F.round(F.min("temperatura"), 2).alias("minima"))
           .orderBy("anio", "mes"))
por_mes.show(36)

print("4.5 Temperatura promedio por franja horaria:")
(df.groupBy("franja_horaria")
   .agg(F.round(F.avg("temperatura"), 2).alias("promedio"),
        F.count("*").alias("lecturas"))
   .orderBy("franja_horaria").show())

print("4.6 Estaciones por piso térmico (según su temperatura promedio):")
por_estacion = (df.groupBy("codigoestacion", "nombreestacion",
                           "departamento", "municipio")
                .agg(F.round(F.avg("temperatura"), 2).alias("promedio"),
                     F.count("*").alias("lecturas"))
                .filter(F.col("lecturas") >= 24)   # mínimo un día de datos
                # Pisos térmicos de Colombia (aproximación por temperatura)
                .withColumn("piso_termico",
                            F.when(F.col("promedio") >= 24, "Cálido")
                             .when(F.col("promedio") >= 17.5, "Templado")
                             .when(F.col("promedio") >= 12, "Frío")
                             .otherwise("Páramo")))
(por_estacion.groupBy("piso_termico")
   .agg(F.count("*").alias("estaciones"),
        F.round(F.avg("promedio"), 2).alias("temp_promedio"))
   .orderBy(F.desc("temp_promedio")).show())

print("    Las 10 estaciones más cálidas:")
por_estacion.orderBy(F.desc("promedio")).show(10, truncate=False)
print("    Las 10 estaciones más frías:")
por_estacion.orderBy("promedio").show(10, truncate=False)

print(f"4.7 Municipios con más lecturas de calor extremo (>= {UMBRAL_CALOR} °C):")
calor_extremo = (df.filter(F.col("temperatura") >= UMBRAL_CALOR)
                 .groupBy("departamento", "municipio")
                 .agg(F.count("*").alias("lecturas_extremas"),
                      F.round(F.max("temperatura"), 2).alias("maxima"))
                 .orderBy(F.desc("lecturas_extremas")))
calor_extremo.show(10, truncate=False)

# 4.8 Detección de anomalías con funciones de ventana (Window):
#     z = (temperatura - promedio_estación) / desviación_estación
print(f"4.8 Lecturas anómalas por estación (|z| > {UMBRAL_Z}):")
ventana_est = Window.partitionBy("codigoestacion")
df_z = (df.withColumn("prom_est", F.avg("temperatura").over(ventana_est))
          .withColumn("std_est", F.stddev("temperatura").over(ventana_est))
          .withColumn("z", (F.col("temperatura") - F.col("prom_est"))
                      / F.col("std_est")))
anomalias = df_z.filter(F.abs(F.col("z")) > UMBRAL_Z)
total_anomalias = anomalias.count()
print(f"Lecturas anómalas detectadas: {total_anomalias:,} "
      f"({total_anomalias * 100 / max(total_limpio, 1):.3f} % del total)")
(anomalias.select("nombreestacion", "departamento", "fecha", "temperatura",
                  F.round("prom_est", 2).alias("prom_estacion"),
                  F.round("z", 2).alias("z"))
 .orderBy(F.desc(F.abs(F.col("z")))).show(10, truncate=False))

# 4.9 Consulta con Spark SQL
print("4.9 Spark SQL: amplitud térmica diaria promedio por departamento")
df.createOrReplaceTempView("temperatura")
amplitud = spark.sql("""
    SELECT departamento,
           ROUND(AVG(t_max - t_min), 2) AS amplitud_diaria_prom,
           COUNT(*)                     AS dias_estacion
    FROM (
        SELECT departamento, codigoestacion, dia,
               MAX(temperatura) AS t_max, MIN(temperatura) AS t_min
        FROM temperatura
        GROUP BY departamento, codigoestacion, dia
        HAVING COUNT(*) >= 12
    ) t
    GROUP BY departamento
    ORDER BY amplitud_diaria_prom DESC
""")
amplitud.show(10, truncate=False)

# ---------------------------------------------------------------------------
# 6. Las mismas ideas con RDDs (transformaciones y acciones)
# ---------------------------------------------------------------------------
titulo("5. OPERACIONES CON RDDs")
rdd = df.select("departamento", "temperatura").rdd \
        .map(lambda fila: (fila[0], fila[1]))           # transformación map

print("take(3):", rdd.take(3))                           # acción take

calurosas = rdd.filter(lambda x: x[1] >= 30)             # transformación filter
print("count() lecturas >= 30 °C:", calurosas.count())   # acción count

# reduceByKey: suma y conteo por departamento para obtener el promedio
suma_conteo = (rdd.map(lambda x: (x[0], (x[1], 1)))
                  .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])))
promedios_rdd = suma_conteo.mapValues(lambda v: round(v[0] / v[1], 2))
print("Promedio por departamento (RDD + reduceByKey), top 5:")
for depto, prom in promedios_rdd.sortBy(lambda x: -x[1]).take(5):
    print(f"   {depto:<35} {prom:>6} °C")

maxima = rdd.map(lambda x: x[1]).reduce(lambda a, b: max(a, b))  # acción reduce
print("reduce() temperatura máxima registrada:", maxima, "°C")

# ---------------------------------------------------------------------------
# 7. Guardar resultados en HDFS
# ---------------------------------------------------------------------------
titulo("6. ALMACENAMIENTO DE RESULTADOS")
try:
    por_depto.coalesce(1).write.mode("overwrite") \
        .option("header", "true").csv(RUTA_SALIDA + "/por_departamento")
    por_hora.coalesce(1).write.mode("overwrite") \
        .option("header", "true").csv(RUTA_SALIDA + "/por_hora")
    por_mes.coalesce(1).write.mode("overwrite") \
        .option("header", "true").csv(RUTA_SALIDA + "/por_mes")
    print(f"Resultados guardados en {RUTA_SALIDA}")
except Exception as error:  # p. ej. si HDFS no está iniciado
    print("No fue posible guardar en", RUTA_SALIDA, "->", error)

# ---------------------------------------------------------------------------
# 8. Visualización de resultados (matplotlib)
#    Solo se llevan al driver (toPandas) las tablas YA agregadas, que son
#    pequeñas; nunca el conjunto de datos completo.
# ---------------------------------------------------------------------------
titulo("7. VISUALIZACIÓN")
try:
    import matplotlib
    matplotlib.use("Agg")           # permite generar imágenes sin pantalla
    import matplotlib.pyplot as plt

    os.makedirs(CARPETA_GRAFICAS, exist_ok=True)

    # Gráfica 1: promedio por departamento
    pd_depto = por_depto.toPandas().sort_values("promedio")
    plt.figure(figsize=(10, max(4, 0.3 * len(pd_depto))))
    plt.barh(pd_depto["departamento"], pd_depto["promedio"], color="#d9622b")
    plt.xlabel("Temperatura promedio (°C)")
    plt.title("Temperatura promedio del aire por departamento - IDEAM")
    plt.tight_layout()
    plt.savefig(f"{CARPETA_GRAFICAS}/1_promedio_departamento.png", dpi=120)
    plt.close()

    # Gráfica 2: ciclo diario
    pd_hora = por_hora.toPandas()
    plt.figure(figsize=(9, 4.5))
    plt.plot(pd_hora["hora"], pd_hora["promedio"], marker="o",
             color="#1f6fb2")
    plt.xticks(range(0, 24))
    plt.xlabel("Hora del día")
    plt.ylabel("Temperatura promedio (°C)")
    plt.title("Ciclo diario de la temperatura del aire")
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{CARPETA_GRAFICAS}/2_ciclo_diario.png", dpi=120)
    plt.close()

    # Gráfica 3: evolución mensual (promedio, máxima y mínima)
    pd_mes = por_mes.toPandas()
    etiquetas = [f"{a}-{m:02d}" for a, m in zip(pd_mes["anio"], pd_mes["mes"])]
    plt.figure(figsize=(10, 4.5))
    plt.plot(etiquetas, pd_mes["maxima"], label="Máxima", color="#c0392b")
    plt.plot(etiquetas, pd_mes["promedio"], label="Promedio", color="#555555")
    plt.plot(etiquetas, pd_mes["minima"], label="Mínima", color="#2471a3")
    plt.xticks(rotation=45)
    plt.ylabel("°C")
    plt.title("Temperatura mensual registrada por la red IDEAM")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{CARPETA_GRAFICAS}/3_evolucion_mensual.png", dpi=120)
    plt.close()

    # Gráfica 4: histograma (se calcula en Spark, no en pandas)
    bordes = list(range(-10, 46, 2))
    conteos = (df.select("temperatura").rdd.map(lambda r: r[0])
                 .histogram(bordes)[1])
    plt.figure(figsize=(9, 4.5))
    plt.bar(bordes[:-1], conteos, width=2, align="edge",
            color="#7d3c98", edgecolor="white")
    plt.xlabel("Temperatura (°C)")
    plt.ylabel("Número de lecturas")
    plt.title("Distribución de las lecturas de temperatura")
    plt.tight_layout()
    plt.savefig(f"{CARPETA_GRAFICAS}/4_histograma.png", dpi=120)
    plt.close()

    print(f"Gráficas guardadas en la carpeta ./{CARPETA_GRAFICAS}/")
except ImportError:
    print("matplotlib no está instalado. Instálelo con:"
          "  pip install matplotlib pandas")

titulo("FIN DEL PROCESAMIENTO BATCH")
df.unpersist()
spark.stop()
