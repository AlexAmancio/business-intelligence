"""
Genera un dataset sintetico de estadias de hotel a gran escala, calibrado
con las proporciones observadas en la base real 'alquiler' (21 registros):
FINALIZADA 47.6%, ACTIVA 38.1%, CANCELADA 9.5%, NO_SHOW 4.8%; y con la
diferencia por dia de semana y categoria de habitacion que se ve en el
datamart (mas cancelaciones los viernes/lunes, mas no-shows en Deluxe).

El objetivo no es replicar el numero exacto, sino generar un dataset con
volumen real (millones de filas) que conserve senal (no ruido puro) para
que el analisis de churn en PySpark tenga algo que encontrar.
"""

import csv
import random
import datetime
import sys

random.seed(42)

N = int(sys.argv[1]) if len(sys.argv) > 1 else 3_000_000
OUT = sys.argv[2] if len(sys.argv) > 2 else "estadias_sinteticas.csv"

CATEGORIAS = [
    ("Habitación Estándar", 150.0, 0.10),
    ("Habitación Deluxe", 260.0, 0.18),
    ("Suite Junior", 380.0, 0.14),
]
PESOS_CATEGORIA = [0.55, 0.30, 0.15]

DIAS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
FACTOR_DIA = {
    "Monday": 1.4, "Tuesday": 0.8, "Wednesday": 0.7, "Thursday": 0.9,
    "Friday": 1.5, "Saturday": 0.6, "Sunday": 0.9,
}

FECHA_INICIO = datetime.date(2024, 1, 1)
DIAS_RANGO = (datetime.date(2026, 12, 31) - FECHA_INICIO).days

PAISES = ["Perú", "Chile", "Colombia", "Ecuador", "Argentina", "México", "España", "Estados Unidos"]
OCUPACIONES = ["Ingeniero", "Comerciante", "Docente", "Estudiante", "Médico", "Abogado", "Turista", "Freelancer"]


def generar_fila(i):
    categoria, precio, no_show_base = random.choices(CATEGORIAS, weights=PESOS_CATEGORIA, k=1)[0]

    dias_offset = random.randint(0, DIAS_RANGO)
    fecha_checkin = FECHA_INICIO + datetime.timedelta(days=dias_offset)
    dia_semana = DIAS[fecha_checkin.weekday()]
    hora = random.randint(12, 20)

    noches = random.choices([1, 2, 3, 4, 5, 7], weights=[30, 25, 20, 12, 8, 5])[0]
    huespedes = random.choices([1, 2, 3, 4], weights=[35, 40, 15, 10])[0]

    anticipacion_horas = random.choices(
        [random.randint(1, 6), random.randint(6, 48), random.randint(48, 240), random.randint(240, 1440)],
        weights=[15, 30, 35, 20],
    )[0]

    p_no_show = no_show_base * FACTOR_DIA[dia_semana] * (1.3 if anticipacion_horas < 6 else 1.0)
    p_cancelada = 0.09 * FACTOR_DIA[dia_semana] * (1.5 if anticipacion_horas > 240 else 0.8)

    r = random.random()
    if r < p_no_show:
        estado = "NO_SHOW"
    elif r < p_no_show + p_cancelada:
        estado = "CANCELADA"
    else:
        estado = random.choices(["FINALIZADA", "ACTIVA"], weights=[85, 15])[0]

    tarifa = round(precio * noches, 2)
    consumos = round(random.choices([0, 0, 0, 35, 60, 120, 200], weights=[40, 15, 10, 15, 10, 6, 4])[0], 2)
    monto_pagado = tarifa if estado == "FINALIZADA" else round(tarifa * random.choice([0, 0.3, 0.5, 1.0]), 2)

    return {
        "id_registro": i,
        "categoria": categoria,
        "fecha_checkin": fecha_checkin.isoformat(),
        "hora_checkin": hora,
        "dia_semana": dia_semana,
        "noches": noches,
        "num_huespedes": huespedes,
        "anticipacion_horas": anticipacion_horas,
        "tarifa": tarifa,
        "consumos": consumos,
        "monto_pagado": monto_pagado,
        "pais_cliente": random.choice(PAISES),
        "ocupacion_cliente": random.choice(OCUPACIONES),
        "estado": estado,
    }


def main():
    campos = list(generar_fila(0).keys())
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        for i in range(1, N + 1):
            writer.writerow(generar_fila(i))
            if i % 500_000 == 0:
                print(f"  {i:,} filas generadas...")
    print(f"Listo: {N:,} filas en {OUT}")


if __name__ == "__main__":
    main()
