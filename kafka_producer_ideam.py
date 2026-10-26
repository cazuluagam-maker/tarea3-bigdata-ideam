# -*- coding: utf-8 -*-
"""
Tarea 3 - Big Data UNAD - Procesamiento en TIEMPO REAL
Productor (producer) de Kafka: simula estaciones meteorológicas automáticas
que envían lecturas de temperatura del aire, con la misma estructura de
campos del conjunto de datos del IDEAM (datos.gov.co, sbwg-7ju4).

Autor: Carlos Alberto Zuluaga Morales - Grupo 106

Ejecución:  python3 kafka_producer_ideam.py
Detener:    Ctrl + C
"""

import json
import math
import random
import time
from datetime import datetime

from kafka import KafkaProducer

TOPIC = "temperatura_ideam"
SERVIDOR = "localhost:9092"
PAUSA_SEGUNDOS = 1          # tiempo entre cada lectura enviada
PROB_ERROR_SENSOR = 0.03    # 3 % de lecturas con falla de sensor
PROB_OLA_CALOR = 0.05       # 5 % de lecturas con calor extremo

# Estaciones simuladas: (codigo, nombre, departamento, municipio, temp_base °C)
# La temperatura base es aproximada al clima típico de cada municipio.
ESTACIONES = [
    ("SIM-001", "ESTACION BOGOTA - AUT", "BOGOTA D.C.", "BOGOTA, D.C", 14.0),
    ("SIM-002", "ESTACION MEDELLIN - AUT", "ANTIOQUIA", "MEDELLIN", 22.5),
    ("SIM-003", "ESTACION CALI - AUT", "VALLE DEL CAUCA", "CALI", 24.5),
    ("SIM-004", "ESTACION CARTAGENA - AUT", "BOLIVAR", "CARTAGENA", 28.0),
    ("SIM-005", "ESTACION VALLEDUPAR - AUT", "CESAR", "VALLEDUPAR", 29.0),
    ("SIM-006", "ESTACION RIOHACHA - AUT", "LA GUAJIRA", "RIOHACHA", 28.5),
    ("SIM-007", "ESTACION TUNJA - AUT", "BOYACA", "TUNJA", 13.0),
    ("SIM-008", "ESTACION NEIVA - AUT", "HUILA", "NEIVA", 28.0),
]


def generar_lectura():
    """Genera una lectura de temperatura para una estación al azar."""
    codigo, nombre, depto, municipio, base = random.choice(ESTACIONES)
    ahora = datetime.now()

    # Ciclo diario: más frío a las 4-5 a. m., más caliente hacia las 3 p. m.
    ciclo = 4 * math.sin((ahora.hour + ahora.minute / 60 - 9) / 24 * 2 * math.pi)
    temperatura = base + ciclo + random.gauss(0, 0.8)

    azar = random.random()
    if azar < PROB_ERROR_SENSOR:
        temperatura = random.choice([-99.0, 99.9])      # lectura errónea
    elif azar < PROB_ERROR_SENSOR + PROB_OLA_CALOR:
        temperatura += random.uniform(5, 9)             # pico de calor

    return {
        "codigoestacion": codigo,
        "nombreestacion": nombre,
        "departamento": depto,
        "municipio": municipio,
        "valorobservado": round(temperatura, 2),
        "unidadmedida": "°C",
        "fechaobservacion": ahora.strftime("%Y-%m-%dT%H:%M:%S"),
    }


productor = KafkaProducer(
    bootstrap_servers=[SERVIDOR],
    # La clave (código de estación) hace que todas las lecturas de una misma
    # estación vayan siempre a la misma partición del topic.
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)

print(f"Enviando lecturas al topic '{TOPIC}'... (Ctrl + C para detener)")
enviados = 0
try:
    while True:
        lectura = generar_lectura()
        productor.send(TOPIC, key=lectura["codigoestacion"], value=lectura)
        enviados += 1
        print(f"[{enviados}] Enviado: {lectura['fechaobservacion']} | "
              f"{lectura['departamento']:<16} | {lectura['valorobservado']:>6} °C")
        time.sleep(PAUSA_SEGUNDOS)
except KeyboardInterrupt:
    print(f"\nProductor detenido. Total de mensajes enviados: {enviados}")
finally:
    productor.flush()
    productor.close()
