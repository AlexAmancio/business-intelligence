-- =====================================================================
-- DATAMART DE ESTADIAS - HOTEL TUNKI
-- Origen OLTP: alquiler (MySQL, Sistema Estadia)
-- Destino OLAP: hotel_dw (modelo estrella)
-- Pregunta de negocio: por que los huespedes no llegan (no-show y
-- cancelacion) y como se relaciona eso con habitacion, cliente y fecha.
-- =====================================================================

DROP DATABASE IF EXISTS hotel_dw;
CREATE DATABASE hotel_dw;
USE hotel_dw;

-- ---------------------------------------------------------------------
-- DIMENSIONES
-- ---------------------------------------------------------------------

CREATE TABLE dim_tiempo (
    id_tiempo     INT PRIMARY KEY,          -- formato AAAAMMDD
    fecha         DATE NOT NULL,
    anio          INT,
    mes           INT,
    nombre_mes    VARCHAR(20),
    dia           INT,
    dia_semana    VARCHAR(15),
    es_fin_semana TINYINT(1),
    trimestre     INT
);

CREATE TABLE dim_categoria (
    id_categoria  INT PRIMARY KEY,
    nombre        VARCHAR(50),
    precio_base   DECIMAL(10,2)
);

CREATE TABLE dim_habitacion (
    id_habitacion INT PRIMARY KEY,
    numero        VARCHAR(10),
    piso          INT,
    aforo         INT,
    id_categoria  INT REFERENCES dim_categoria(id_categoria)
);

CREATE TABLE dim_cliente (
    id_cliente    INT PRIMARY KEY,
    nombres       VARCHAR(255),
    apellidos     VARCHAR(255),
    pais          VARCHAR(255),
    ciudad        VARCHAR(255),
    ocupacion     VARCHAR(255)
);

CREATE TABLE dim_usuario (
    id_usuario    INT PRIMARY KEY,
    nombres       VARCHAR(255),
    apellidos     VARCHAR(255),
    es_admin      TINYINT(1)
);

-- ---------------------------------------------------------------------
-- HECHOS (grano: una fila por registro de estadia)
-- ---------------------------------------------------------------------

CREATE TABLE fact_estadia (
    id_registro         INT PRIMARY KEY,
    id_cliente          INT REFERENCES dim_cliente(id_cliente),
    id_habitacion       INT REFERENCES dim_habitacion(id_habitacion),
    id_usuario          INT REFERENCES dim_usuario(id_usuario),
    id_tiempo_checkin   INT REFERENCES dim_tiempo(id_tiempo),
    noches              INT,
    num_huespedes       INT,
    tarifa              DECIMAL(10,2),
    monto_pagado        DECIMAL(10,2),
    total_consumos      DECIMAL(10,2),
    saldo_pendiente      DECIMAL(10,2),
    estado              VARCHAR(20),
    es_no_show          TINYINT(1),
    es_cancelada        TINYINT(1),
    es_finalizada       TINYINT(1),
    horas_anticipacion_checkin INT   -- horas entre registro y checkin planeado
);

-- =====================================================================
-- ETL: EXTRACT + TRANSFORM + LOAD desde alquiler
-- =====================================================================

-- dim_tiempo, generada a partir de los check_in reales
INSERT INTO dim_tiempo (id_tiempo, fecha, anio, mes, nombre_mes, dia, dia_semana, es_fin_semana, trimestre)
SELECT DISTINCT
    CAST(DATE_FORMAT(check_in, '%Y%m%d') AS UNSIGNED) AS id_tiempo,
    DATE(check_in) AS fecha,
    YEAR(check_in), MONTH(check_in), MONTHNAME(check_in), DAY(check_in),
    DAYNAME(check_in),
    IF(DAYOFWEEK(check_in) IN (1,7), 1, 0),
    QUARTER(check_in)
FROM alquiler.registro
WHERE check_in IS NOT NULL;

INSERT INTO dim_categoria (id_categoria, nombre, precio_base)
SELECT id, nombre, precio FROM alquiler.categoria;

INSERT INTO dim_habitacion (id_habitacion, numero, piso, aforo, id_categoria)
SELECT id, numero, piso, aforo, categoria_id FROM alquiler.habitacion;

INSERT INTO dim_cliente (id_cliente, nombres, apellidos, pais, ciudad, ocupacion)
SELECT id, nombres, apellidos, pais, ciudad, ocupacion FROM alquiler.cliente;

INSERT INTO dim_usuario (id_usuario, nombres, apellidos, es_admin)
SELECT id, nombres, apellidos, es_admin FROM alquiler.usuario;

INSERT INTO fact_estadia (
    id_registro, id_cliente, id_habitacion, id_usuario, id_tiempo_checkin,
    noches, num_huespedes, tarifa, monto_pagado, total_consumos, saldo_pendiente,
    estado, es_no_show, es_cancelada, es_finalizada, horas_anticipacion_checkin
)
SELECT
    r.id,
    r.cliente_id,
    r.habitacion_id,
    r.usuario_id,
    CAST(DATE_FORMAT(r.check_in, '%Y%m%d') AS UNSIGNED),
    r.noches,
    r.num_huespedes,
    r.tarifa,
    IFNULL(r.monto_pagado, 0),
    IFNULL((SELECT SUM(c.monto) FROM alquiler.consumo c WHERE c.registro_id = r.id), 0),
    (r.tarifa + IFNULL((SELECT SUM(c.monto) FROM alquiler.consumo c WHERE c.registro_id = r.id), 0)) - IFNULL(r.monto_pagado, 0),
    r.estado,
    IF(r.estado = 'NO_SHOW', 1, 0),
    IF(r.estado = 'CANCELADA', 1, 0),
    IF(r.estado = 'FINALIZADA', 1, 0),
    NULL
FROM alquiler.registro r;

-- =====================================================================
-- CONSULTAS OLAP DE EJEMPLO (responden la pregunta de negocio)
-- =====================================================================

-- Tasa de no-show / cancelacion por categoria de habitacion
SELECT
    cat.nombre AS categoria,
    COUNT(*) AS total_estadias,
    SUM(f.es_no_show) AS no_shows,
    SUM(f.es_cancelada) AS canceladas,
    ROUND(100 * (SUM(f.es_no_show) + SUM(f.es_cancelada)) / COUNT(*), 1) AS pct_no_llega
FROM fact_estadia f
JOIN dim_habitacion h ON f.id_habitacion = h.id_habitacion
JOIN dim_categoria cat ON h.id_categoria = cat.id_categoria
GROUP BY cat.nombre
ORDER BY pct_no_llega DESC;

-- Tasa de no-show / cancelacion por dia de la semana del check-in
SELECT
    t.dia_semana,
    COUNT(*) AS total_estadias,
    SUM(f.es_no_show) AS no_shows,
    SUM(f.es_cancelada) AS canceladas,
    ROUND(100 * (SUM(f.es_no_show) + SUM(f.es_cancelada)) / COUNT(*), 1) AS pct_no_llega
FROM fact_estadia f
JOIN dim_tiempo t ON f.id_tiempo_checkin = t.id_tiempo
GROUP BY t.dia_semana
ORDER BY pct_no_llega DESC;

-- Saldo pendiente total y consumos por estado de la estadia
SELECT
    estado,
    COUNT(*) AS n,
    ROUND(SUM(tarifa), 2) AS ingresos_por_tarifa,
    ROUND(SUM(total_consumos), 2) AS ingresos_por_consumos,
    ROUND(SUM(saldo_pendiente), 2) AS saldo_pendiente_total
FROM fact_estadia
GROUP BY estado;
