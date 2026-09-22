# Estadía — Sistema Hotel

web de check-in / check-out para hoteles,

## ¿Qué hace?

- Registro de check-in y check-out de huéspedes, con cálculo automático de tarifa,
  noches y saldo pendiente.
- Reservas múltiples: una reserva puede cubrir varias habitaciones a la vez, cada una
  con su propio aforo y huéspedes.
- Gestión de clientes, habitaciones, categorías y consumos adicionales durante la estadía.
- Emisión de comprobantes internos (demostración, no válidos ante SUNAT).
- Usuarios con roles (admin / staff) y control de acceso por Spring Security, incluyendo
  bloqueo temporal por intentos fallidos de login.

## Stack

- Java 17, Spring Boot 3.1.4
- Spring Data JPA (Hibernate), Spring Security 6, Thymeleaf
- MySQL

## Cómo levantarlo

**1. Base de datos**

Creá una base de datos MySQL llamada `alquiler` (o cambiá el nombre en
`application.properties`). Por defecto se conecta a `root` sin contraseña en
`localhost:3306`.

**2. Usuario de prueba**

Para poder loguearte, insertá un usuario administrador:

```sql
insert into usuario (apellidos, contrasena, correo, es_admin, nombres, username)
values ('Musk', '$2a$12$gx4jBU180qN4ohLJiyokkuRMZpnhtVmlsVJoCQFrKIeHaXTjI5m5G', 'musk@gmail.com', true, 'Elon', 'elon');
```

Usuario: `elon` — Contraseña: `admin123`

**3. Arrancar la app**

```bash
./mvnw spring-boot:run
```

La app queda en `http://localhost:8090`.

## Con Docker

```bash
docker build -t sistema-hotel .
docker run -p 8090:8090 sistema-hotel
```

Requiere una base MySQL accesible desde el contenedor (ajustá
`spring.datasource.url` según corresponda).

## Tests

```bash
./mvnw test
```

## Proyecto de Business Intelligence y Big Data

Sobre este mismo sistema se construyó el proyecto final del curso Base de Datos
Avanzadas y Big Data (Grupo 4), que analiza por qué los huéspedes no llegan a
completar su estadía (no-show y cancelación).

- `informe/informe_final.docx` — informe oficial (capítulos I a III).
- `bi/datamart_estadia.sql` — ETL y datamart en esquema estrella (`hotel_dw`) sobre
  los datos reales de `alquiler`.
- `bi/arquitectura.png` — diagrama de la arquitectura integrada.
- `bigdata/generar_dataset_sintetico.py` — genera un dataset sintético a escala
  (3,000,000 de filas), calibrado con las proporciones reales.
- `bigdata/analisis_pyspark.py` — procesa el dataset sintético con PySpark
  (local[*]) y corre un benchmark de escalabilidad por número de particiones.
- `bigdata/resultados_pyspark.json` — resultados de la última ejecución.

Para reproducir la parte Big Data: `python3 bigdata/generar_dataset_sintetico.py`
seguido de `python3 bigdata/analisis_pyspark.py` (requiere PySpark y Java 17). El
CSV generado no se versiona por su tamaño (~290 MB).
