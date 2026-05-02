/*
========================================================================================
PROYECTO: Engine de Trazabilidad Logística-CX (Nexu-OTIF)
AUTOR: Sebastián Santos | Industrial Engineering (UNI)
OBJETIVO: Unificar la percepción del cliente (Medallia) con el cumplimiento real 
          de despacho (TRF OTIF) preservando la integridad de la encuesta.
          
LÓGICA CLAVE: 
1. Normalización de documentos mediante Regex para asegurar el match multi-llave.
2. Gestión de cardinalidad: Se utiliza ROW_NUMBER() para evitar la "explosión de filas"
   seleccionando la fila logística con mayor criticidad (peor retraso).
3. Universo: Canal VAD (DAD y RT).
========================================================================================
*/
-- 1) TRF con filtro de partición (en la lectura)
WITH reservas_root AS (
  SELECT
    t.*,
    DATE(t.reservation_generation_dt) AS FECHA_CREACION_D,
    SAFE_CAST(t.original_agreed_delivery_dt AS DATE) AS FECHA_PACTADA_D,
    COALESCE(SAFE_CAST(t.delivered_dttm AS DATE), SAFE_CAST(t.delivery_date_dt AS DATE)) AS FECHA_ENTREGA_D
  FROM `sodone.trf_pe.ontime_and_infull` AS t
  WHERE t.country_id = 'PE'
    AND DATE(t.reservation_generation_dt) >= DATE '2024-01-01'
),

-- 2) Métricas por reserva + estado promesa (aún a nivel línea)
reservas_base AS (
  SELECT
    r.*,
    COUNT(DISTINCT r.services_type) OVER (PARTITION BY r.reservation_num) AS Contar_distintos_tipos_entrega,
    COUNT(DISTINCT r.FECHA_ENTREGA_D) OVER (PARTITION BY r.reservation_num) AS Contar_distintas_fechas_entrega,
    COALESCE(
      CAST(r.request_compliance_flag AS INT64),
      CAST(r.line_ok AS INT64),
      CASE
        WHEN r.FECHA_ENTREGA_D IS NULL THEN 0
        WHEN r.FECHA_ENTREGA_D <= r.FECHA_PACTADA_D THEN 1
        ELSE 0
      END
    ) AS ON_TIME_FLAG
  FROM reservas_root r
),

-- 3) Normalizamos documentos (doc1/doc2)
reservas_norm AS (
  SELECT
    b.*,
    REGEXP_REPLACE(IFNULL(CAST(b.sales_return_doc1_num AS STRING), ''), r'\D', '')                             AS ret1_digits,
    REGEXP_REPLACE(REGEXP_REPLACE(IFNULL(CAST(b.sales_return_doc1_num AS STRING), ''), r'\D', ''), r'^0+', '') AS ret1_norm,
    REGEXP_REPLACE(IFNULL(CAST(b.sales_return_doc2_num AS STRING), ''), r'\D', '')                             AS ret2_digits,
    REGEXP_REPLACE(REGEXP_REPLACE(IFNULL(CAST(b.sales_return_doc2_num AS STRING), ''), r'\D', ''), r'^0+', '') AS ret2_norm
  FROM reservas_base b
),

-- 4) Consolidamos UNA FILA POR DOCUMENTO (doc1 o doc2) para evitar multiplicidad
--    Elegimos la "mejor" fila por documento (regla: peor retraso/entrega nula primero).
reservas_doc_pick AS (
  SELECT
    *,
    COALESCE(ret1_norm, ret2_norm) AS doc_norm,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(ret1_norm, ret2_norm)
      ORDER BY
        IF(FECHA_ENTREGA_D IS NULL, 1, 0) DESC,
        COALESCE(DATE_DIFF(FECHA_ENTREGA_D, FECHA_PACTADA_D, DAY), -999999) DESC,
        FECHA_ENTREGA_D DESC,
        FECHA_PACTADA_D DESC
    ) AS rn_doc
  FROM reservas_norm
  WHERE ret1_norm IS NOT NULL OR ret2_norm IS NOT NULL
),

-- Dejamos sólo UNA fila por documento (doc_norm)
reservas_1doc AS (
  SELECT *
  FROM reservas_doc_pick
  WHERE rn_doc = 1
),

-- 5) Medallia: filtros operativos + 1 encuesta por surveyid (no reducimos el universo)
medallia_raw AS (
  SELECT *
  FROM `sodfunsx.experiencia_pe.data_genera_view`
  WHERE canal_venta_general = 'VAD'
    AND e_status = 'COMPLETED'
    AND e_sdm_punto_contacto IN ('7','10')         -- 7 = DAD, 10 = RT
    AND DATE(e_responsedate) >= DATE '2024-01-01'
),

medallia_norm AS (
  SELECT
    m.*,
    REGEXP_REPLACE(IFNULL(m.e_sdm_transaction_number_text, ''), r'\D', '')                             AS tx_digits,
    REGEXP_REPLACE(REGEXP_REPLACE(IFNULL(m.e_sdm_transaction_number_text, ''), r'\D', ''), r'^0+', '') AS tx_norm
  FROM medallia_raw m
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY m.a_surveyid
    ORDER BY m.e_responsedate DESC
  ) = 1
)

-- 6) JOIN FINAL: preservar 100% Medallia, TRF sólo aporta si hay match 1:1 por doc
SELECT
  -- Llaves
  r.reservation_num           AS NUM_RESERVA,
  r.sales_order_num           AS orderNumber,
  m.a_surveyid,

  -- Fechas
  r.FECHA_CREACION_D,
  r.FECHA_PACTADA_D,
  r.FECHA_ENTREGA_D,
  m.e_responsedate,

  -- Estado / Tipos
  r.reservation_type_desc         AS TIPO_RESERVA,
  r.delivery_main_status_type_desc AS ESTADO_RESERVA,
  r.services_type                 AS TIPO_SERVICIO,
  r.sub_services_type             AS SUB_TIPO_SERVICIO,

  -- Canales / Locales
  r.channel_delivery_desc     AS CANAL_ENTREGA,
  r.channel_desc              AS CANAL_VENTA,
  r.format_from_location_id   AS LOCAL_ORIGEN,
  r.from_location_name        AS nombre_cc_origen,
  NULL                        AS cyber,
  r.delivery_status_type_desc,
  r.reservation_type_indicator,
  r.format_to_location_id     AS LOCAL_DESPACHO,
  r.stock_type_desc           AS ABASTECIMIENTO_ORIGEN,

  -- Ubicaciones
  r.dispatch_region_name      AS REGION_DESP,
  r.dispatch_county_name      AS NOMBRE_CIUDAD_DESP,
  r.customer_region           AS Region_Real,
  NULL                        AS Provincia_Real,
  r.customer_city             AS Distrito_Real,
  r.customer_zone             AS Zona_Entrega,

  -- Producto (no vienen en TRF)
  NULL AS FAMILIA,
  NULL AS SUBFAMILIA,
  NULL AS GRUPO,
  NULL AS ORIGEN,
  NULL AS Costo_Envio_FCOM,

  -- Cálculos de retrasos
  DATE_DIFF(r.FECHA_PACTADA_D, r.FECHA_CREACION_D, DAY) AS DIAS_HASTA_FECHA_PROMETIDA,
  DATE_DIFF(r.FECHA_ENTREGA_D, r.FECHA_PACTADA_D, DAY)  AS DIAS_DE_RETRASO_INCUMPLIMIENTO,

  -- Estados de promesa
  IF(r.ON_TIME_FLAG = 1, 'CUMPLIÓ PROMESA', 'PROMESA INCUMPLIDA') AS estado_promesa,
  CASE
    WHEN r.FECHA_ENTREGA_D IS NULL THEN 'PROMESA INCUMPLIDA (NO ENTREGADO)'
    WHEN DATE_DIFF(r.FECHA_ENTREGA_D, r.FECHA_PACTADA_D, DAY) > 0 THEN 'PROMESA INCUMPLIDA'
    ELSE 'CUMPLIÓ PROMESA'
  END AS estado_promesa_por_fecha,

  -- Dispersión de entrega
  CASE WHEN r.Contar_distintos_tipos_entrega  > 1 THEN 'PRODUCTOS SEPARADOS' ELSE 'PRODUCTOS JUNTOS' END AS Tipo_despacho,
  CASE WHEN r.Contar_distintas_fechas_entrega > 1 THEN 'FECHAS DISTINTAS DE ENTREGA' ELSE 'UNA MISMA FECHA DE ENTREGA' END AS Tipo_entrega,

  ---ORIGEN DEL PRODUCTO SEGÚN RESERVAS
  r.origin_inventory_line AS Origen_producto,

  -- Medallia
  CASE
    WHEN m.e_sdm_punto_contacto = '7'  THEN 'DAD'
    WHEN m.e_sdm_punto_contacto = '10' THEN 'RT'
    ELSE 'MIXTO'
  END AS Tipo_VAD,

  CAST(m.q_sdm_ltr11 AS INT64) - 1 AS NPS,

  CAST(m.q_sdm_despacho_domicilio_osat5 AS INT64)                      AS CSAT_Satisfaccion_despacho,
  CAST(m.q_sdm_despacho_disponibilidad_fechas_sat5 AS INT64)           AS CSAT_disponibilidad_fechas_despacho,
  CAST(m.q_sdm_despacho_claridad_comunicaciones_sat5 AS INT64)         AS CSAT_claridad_comunicaciones_despacho,
  CAST(m.q_sdm_despacho_facilidad_informacion_seguimiento_sat5 AS INT64) AS CSAT_informacion_seguimiento_despacho,
  CAST(m.q_sdm_despacho_costo_sat5 AS INT64)                           AS CSAT_costo_despacho,
  CAST(m.q_sdm_despacho_caracteristicas_producto_sat5 AS INT64)        AS CSAT_caracteristicas_producto_despacho,
  CAST(m.q_sdm_despacho_exactitud_entrega_sat5 AS INT64)               AS CSAT_exactitud_entrega_despacho,
  CAST(m.q_sdm_despacho_satisfaccion_transportador_sat5 AS INT64)      AS CSAT_satifaccion_transportista_despacho,

  CAST(m.q_sdm_retiro_tienda_osat5 AS INT64)                           AS CSAT_Satisfaccion_retiro,
  CAST(m.q_sdm_retiro_disponibilidad_fechas_sat5 AS INT64)             AS CSAT_disponbilidad_fechas_retiro,
  CAST(m.q_sdm_retiro_claridad_comunicaciones_sat5 AS INT64)           AS CSAT_claridad_comunicaciones_retiro,
  CAST(m.q_sdm_retiro_facilidad_informacion_seguimiento_sat5 AS INT64) AS CSAT_informacion_seguimiento_retiro,
  CAST(m.q_sdm_retiro_cumplimiento_plazos_entrega_sat5 AS INT64)       AS CSAT_cumplimiento_plazsos_retiro,
  CAST(m.q_sdm_retiro_caracteristicas_producto_sat5 AS INT64)          AS CSAT_caracteristicas_producto_retiro,
  CAST(m.q_sdm_retiro_amabilidad_personal_sat5 AS INT64)               AS CSAT_amabilidad_personal_retiro,
  CAST(m.q_sdm_retiro_rapidez_entrega_sat5 AS INT64)                   AS CSAT_rapidez_entrega_retiro,

--- FAMILIA SEGÚN MEDALLIA - ENCUESTA
  m.e_sdm_department_category_text AS Familia_medallia,
--- USO (CLIENTE HOGAR/PROFESIONAL) SEGÚN MEDALLIA
  m.uso_producto,
  m.plan_canal_nps,
  m.comentario_principal

FROM medallia_norm m
LEFT JOIN reservas_1doc r                    
  ON m.tx_norm = r.doc_norm;
