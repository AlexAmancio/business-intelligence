"""
Procesamiento distribuido con PySpark sobre el dataset sintetico de
estadias (3,000,000 de filas) para responder la pregunta de negocio:
por que los huespedes no llegan (no-show y cancelacion), y como cambia
esa tasa segun categoria de habitacion, dia de la semana y anticipacion
de la reserva.

Ademas registra un benchmark de escalabilidad: el mismo agregado se
corre variando el numero de particiones de Spark (1, 4, 8) para medir
como cambia el tiempo de ejecucion sobre el mismo volumen de datos.
"""

import time
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CSV_PATH = "estadias_sinteticas.csv"
RESULTADOS_PATH = "resultados_pyspark.json"

spark = (
    SparkSession.builder
    .appName("HotelTunki-ChurnAnalysis")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print(f"Nucleos disponibles para Spark local[*]: {spark.sparkContext.defaultParallelism}")

df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)
df = df.withColumn("es_no_show", (F.col("estado") == "NO_SHOW").cast("int"))
df = df.withColumn("es_cancelada", (F.col("estado") == "CANCELADA").cast("int"))
df = df.withColumn("no_llega", ((F.col("estado") == "NO_SHOW") | (F.col("estado") == "CANCELADA")).cast("int"))
df.cache()

total_filas = df.count()
print(f"Total de filas cargadas: {total_filas:,}")

# ---------------------------------------------------------------------
# 1. Tasa de no-llegada por categoria de habitacion
# ---------------------------------------------------------------------
por_categoria = (
    df.groupBy("categoria")
    .agg(
        F.count("*").alias("total"),
        F.sum("es_no_show").alias("no_shows"),
        F.sum("es_cancelada").alias("canceladas"),
        F.round(100 * F.avg("no_llega"), 2).alias("pct_no_llega"),
    )
    .orderBy(F.desc("pct_no_llega"))
    .collect()
)

# ---------------------------------------------------------------------
# 2. Tasa de no-llegada por dia de la semana
# ---------------------------------------------------------------------
por_dia = (
    df.groupBy("dia_semana")
    .agg(
        F.count("*").alias("total"),
        F.round(100 * F.avg("no_llega"), 2).alias("pct_no_llega"),
    )
    .orderBy(F.desc("pct_no_llega"))
    .collect()
)

# ---------------------------------------------------------------------
# 3. Tasa de no-llegada por franja de anticipacion de la reserva
# ---------------------------------------------------------------------
df = df.withColumn(
    "franja_anticipacion",
    F.when(F.col("anticipacion_horas") < 6, "menos de 6h")
     .when(F.col("anticipacion_horas") < 48, "6h a 48h")
     .when(F.col("anticipacion_horas") < 240, "2 a 10 dias")
     .otherwise("mas de 10 dias"),
)
por_anticipacion = (
    df.groupBy("franja_anticipacion")
    .agg(
        F.count("*").alias("total"),
        F.round(100 * F.avg("no_llega"), 2).alias("pct_no_llega"),
    )
    .orderBy(F.desc("pct_no_llega"))
    .collect()
)

# ---------------------------------------------------------------------
# 4. Benchmark de escalabilidad: mismo agregado con distinto numero de
#    particiones, midiendo tiempo real de ejecucion (accion .collect()).
# ---------------------------------------------------------------------
benchmark = []
for n_part in [1, 4, 8, spark.sparkContext.defaultParallelism]:
    df_part = df.repartition(n_part)
    t0 = time.time()
    df_part.groupBy("categoria", "dia_semana").agg(F.avg("no_llega")).collect()
    t1 = time.time()
    benchmark.append({"particiones": n_part, "segundos": round(t1 - t0, 3)})
    print(f"  particiones={n_part:<3} -> {t1 - t0:.3f} s")

resultados = {
    "total_filas": total_filas,
    "nucleos_spark": spark.sparkContext.defaultParallelism,
    "por_categoria": [row.asDict() for row in por_categoria],
    "por_dia_semana": [row.asDict() for row in por_dia],
    "por_anticipacion": [row.asDict() for row in por_anticipacion],
    "benchmark_particiones": benchmark,
}

with open(RESULTADOS_PATH, "w", encoding="utf-8") as f:
    json.dump(resultados, f, indent=2, ensure_ascii=False)

print("\n=== Tasa de no-llegada por categoria ===")
for row in por_categoria:
    print(f"  {row['categoria']:<25} total={row['total']:>8,}  no_show={row['no_shows']:>6,}  cancel={row['canceladas']:>6,}  pct_no_llega={row['pct_no_llega']}%")

print("\n=== Tasa de no-llegada por dia de la semana ===")
for row in por_dia:
    print(f"  {row['dia_semana']:<10} total={row['total']:>8,}  pct_no_llega={row['pct_no_llega']}%")

print("\n=== Tasa de no-llegada por anticipacion de reserva ===")
for row in por_anticipacion:
    print(f"  {row['franja_anticipacion']:<16} total={row['total']:>8,}  pct_no_llega={row['pct_no_llega']}%")

print(f"\nResultados guardados en {RESULTADOS_PATH}")
spark.stop()
