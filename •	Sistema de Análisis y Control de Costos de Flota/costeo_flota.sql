----------------------------------------------------------------------------------------------------------------------------------------------------
---SISTEMA DE ANÁLISIS Y CONTROL DE COSTOS DE FLOTA EN LA ÚLTIMA MILLA
----------------------------------------------------------------------------------------------------------------------------------------------------

---1. NORMALIZAMOS Y DEDUPLICAMOS LA TABLA DE TRANSACCIONES
WITH tabla_transacciones AS (
  SELECT *
  FROM (
    SELECT
      smf.first_origin_document_num,
      smf.document_use_type_cd,
      ROW_NUMBER() OVER (
        PARTITION BY smf.first_origin_document_num
        ORDER BY smf.transaction_dt DESC
      ) AS rn
    FROM `sodone.trf_pe.sales_margin_fact` smf
    WHERE EXTRACT(YEAR FROM transaction_dt) >= 2025
  )
  WHERE rn = 1
),
---2. NORMALIZAMOS Y DEDUPLICAMOS LA TABLA DE RESERVAS
tabla_reservas AS (
  SELECT
    r.NUM_RESERVA,
    r.CC_ORIGEN,
    r.CC_DESPACHA,
    r.Peso_Linea,
    r.Zona_Entrega,
    r.Nombre_region_desp,
    r.sales_return_doc1_num,
    r.Peso_Reserva_Total
  FROM `sod-pe-bi-sandbox.Omnicanal_Pe.Reservas_PE` r
  WHERE EXTRACT(YEAR FROM r.reservation_generation_dt) >= 2025
  AND r.tipo_servicio="DADc"
),

---3. CREAMOS LA FUNCIÓN DE VENTANA PARA NORMALIZAR LA LLAVE DE LAS RUTAS
tabla_simpli_visitas AS (
  SELECT v1.*,
          ---3.1. AQUÍ CONVERTIMOS EL CAMPO VISIT__REFERENCE EN UNA LLAVE QUE PUEDA CRUZAR CON LA BOLETA DETERMINANDA EN 
          ---     EN LA TABLA DE COSTOS
          CASE
          WHEN REGEXP_CONTAINS(v1.visit__reference, r'(B|F)\D*\d')
            THEN CONCAT(
              REGEXP_EXTRACT(v1.visit__reference,r'(B|F)'),
              LPAD(
                REGEXP_EXTRACT(v1.visit__reference, r'(?:B|F)\D*(\d{1,12})'),
                12,
                "0"
              )
            )
          ELSE NULL
        END AS visitas_referencia,
          ---3.2. BUSCAMOS EXTRAER EL COSTO/PRECIO ANOTADO POR EL ASESOR DEL CAMPO NOTAS, TOMAREMOS ESTE COSTO COMO LA PRIMERA 
          ---     PRIORIDAD DEBIDO A ESO TENDR UNA JERARQUIA DE 1 Y LO AGREGARAMOS A LA TABLA COMO CAMPO
          SAFE_CAST(
            REGEXP_EXTRACT(
              v1.visit__notes,
              r'Costo:\s*([0-9]+(?:\.[0-9]+)?)'
            ) AS FLOAT64
          ) AS precio_simpli_1,
          v1.visit__status AS estado_visita
  FROM `sod-pe-bi-sandbox.Omnicanal_SOPE.Bd_Entregas_Simpliroute` AS v1
  WHERE v1.COD IN (91,97)
),

---4.1. CREAMOS UNA VENTANA PARA DETERMINAR EL PRECIO POR FLETE QUE SE LE ASIGNA AL CLIENTE EN SU COMRPRA
reservas_costos_1 AS (
  SELECT
    CAST(R.NUM_RESERVA AS STRING) AS NUM_RESERVA,
    R.sales_return_doc1_num,
    TRX.first_origin_document_num,
    CONCAT(COALESCE(TRX.document_use_type_cd,"F"),
      CASE 
        WHEN LENGTH(TRX.first_origin_document_num) = 9
          THEN LPAD(TRX.first_origin_document_num,12,"0")
        ELSE TRX.first_origin_document_num
      END) AS boleta,
    ROUND(R.PESO_LINEA,1) AS peso_linea,
    R.CC_DESPACHA,
    R.CC_ORIGEN,
    R.ZONA_ENTREGA,
    R.NOMBRE_REGION_DESP,
    R.Peso_Reserva_Total,
    ---4.1.1. CREAMOS UNA SUBCONSULTA INTERNA PARA EXTRAER LOS PRECIOS SEGÚN PESO, ESTA DATA LA TRAEMOS DE UNA TABLA "TARIFARIOS"
    (
      SELECT ANY_VALUE(
        SAFE_CAST(
          CASE
            WHEN R.ZONA_ENTREGA = 'Urbana' THEN SAFE_CAST(T.Urbana AS FLOAT64)
            WHEN R.ZONA_ENTREGA = 'Urbana_Lejana' THEN SAFE_CAST(T.Periferia AS FLOAT64)
            WHEN R.ZONA_ENTREGA = 'Urbana_Playa_Sur' THEN SAFE_CAST(T.Periferia AS FLOAT64)
            WHEN R.ZONA_ENTREGA = 'Periferia' THEN SAFE_CAST(T.Periferia AS FLOAT64)
            ELSE NULL
          END
        AS FLOAT64)
      )
      FROM `sod-pe-bi-sandbox.Omnicanal_SOPE.Tarifario_HD` T
      WHERE
      ---4.1.2. AQUÍ ES DONDE LE DECIMOS EL RANGO DEL PRECIO SEGÚN EL PESO TOTAL DEL SKU X CANTIDAD
        R.PESO_LINEA BETWEEN T.Desde_Kg AND T.Hasta_Kg
      ---4.1.3. CONDICIONAMOS SI ES LIMA O CALLAO CUENTA COMO LIMA, PARA OTROS CASOS CUENTA COMO ENVIOS A PROVINCIA
        AND T.OBS = IF(R.NOMBRE_REGION_DESP IN ("LIMA","PROV CONST DEL CALLAO"),"LIMA","PROVINCIA CON CT (handling+UM), sin troncal")
    ) AS precio_cliente_tarifario
  FROM tabla_reservas R
    LEFT JOIN tabla_transacciones AS TRX
      ON R.sales_return_doc1_num = TRX.first_origin_document_num

),

reservas_costos_agrupados AS (
  SELECT
    NUM_RESERVA,
    sales_return_doc1_num,
    first_origin_document_num,
    boleta,
    ANY_VALUE(CC_DESPACHA) AS CC_DESPACHA,
    ANY_VALUE(CC_ORIGEN) AS CC_ORIGEN,
    ZONA_ENTREGA,
    NOMBRE_REGION_DESP,
    Peso_Reserva_Total,

    -- 🔑 SUMA del costo por línea
    SUM(precio_cliente_tarifario) AS precio_cliente_tarifario

  FROM reservas_costos_1
  GROUP BY
    NUM_RESERVA,
    sales_return_doc1_num,
    first_origin_document_num,
    boleta,
    ZONA_ENTREGA,
    NOMBRE_REGION_DESP,
    Peso_Reserva_Total
),

---4.2. CREAMOS UNA SEGUNDA VENTANA PARA DETERMINAR LOS MISMOS PARAMETROS
reservas_costos_2 AS (
  SELECT *
  FROM reservas_costos_agrupados
),


---4.3. CREAMOS UNA TERCERA VENTANA PARA DETERMINAR LOS MISMOS PARAMETROS
reservas_costos_3 AS (
  SELECT * FROM reservas_costos_agrupados
),

---5. SELECCIONAMOS TODAS LAS COLUMNAS PARA LA TABLA 
base_reservas_visitas AS(
SELECT
      visitas.visit__id,
      visitas.planned_date,
      visitas.vehicle__name,
      COALESCE(costos_1.NUM_RESERVA,
                costos_2.NUM_RESERVA,
                "No se ubicó reserva"
              ) AS Num_reserva,
      
      visitas.visitas_referencia,
      visitas.visit__notes,
      visitas.visit__status,
      costos_2.boleta,
      COALESCE(costos_1.ZONA_ENTREGA,
              costos_2.ZONA_ENTREGA
              ) AS Zona_Entrega,
      COALESCE(costos_1.CC_DESPACHA,
                costos_2.CC_DESPACHA
                ) AS CC_Despacha,
      COALESCE(costos_1.CC_ORIGEN,
                costos_2.CC_ORIGEN
                ) AS CC_Origen,
      COALESCE(costos_1.Peso_Reserva_Total,
                costos_2.Peso_Reserva_Total
                ) AS Peso_Reserva,
      visitas.precio_simpli_1 AS tarifa_1,
      costos_1.precio_cliente_tarifario AS tarifa_2,
      costos_2.precio_cliente_tarifario AS tarifa_3,
      COALESCE(visitas.precio_simpli_1,
                costos_1.precio_cliente_tarifario,
                costos_2.precio_cliente_tarifario
              ) AS tarifario_calculado
FROM tabla_simpli_visitas AS visitas
  LEFT JOIN reservas_costos_agrupados AS costos_1
    ON visitas.visit__reference = costos_1.NUM_RESERVA
  LEFT JOIN reservas_costos_2 AS costos_2
    ON visitas.visitas_referencia = costos_2.boleta
),

base_reservas_visitas_enriquecida AS (
  SELECT
    *,
    --- FLAG: VISITA NO COMPLETADA
    CASE
      WHEN LOWER(visit__status) = "pending" THEN 1
      ELSE 0
    END AS visita_no_completada_flag,
    -- 🔁 Peso final por reserva
    COALESCE(
      Peso_Reserva,
      AVG(Peso_Reserva) OVER (
        PARTITION BY DATE(planned_date), vehicle__name
      )
    ) AS Peso_Reserva_final,

    -- 🔁 Tarifario final por reserva
    COALESCE(
      tarifario_calculado,
      AVG(tarifario_calculado) OVER (
        PARTITION BY DATE(planned_date), vehicle__name
      )
    ) AS tarifario_final

  FROM base_reservas_visitas
),

base_camion AS (
  SELECT
    DATE(planned_date) AS fecha_despacho,
    vehicle__name      AS vehiculo,

    COUNT(DISTINCT visit__id) AS reservas_cargadas,
    COUNTIF(Zona_Entrega IN("Urbana", NULL)) AS Zona_Urbana,
    COUNTIF(Zona_Entrega="Urbana_Playa_Sur") AS Zona_Playa_Sur,
    COUNTIF(Zona_Entrega="Urbana_Lejana") AS Zona_Urbana_Lejana,
    COUNTIF(Zona_Entrega="Periferica") AS Zona_Periferica,
    ---ESTADOS DE LAS VISITAS
    COUNTIF(visit__status="pending") AS Pendientes,
    COUNTIF(visit__status="completed") AS Completos,
    COUNTIF(visit__status="failed") AS Fallidos,
    ROUND(SUM(Peso_Reserva_final),1)          AS peso_total_est,
    ROUND(SUM(base_reservas_visitas_enriquecida.tarifario_final)) AS total_tarifario,
    CC_DESPACHA AS cc_despacha,
    --- ZONA DE ENTREGA NORMALZADA POR VEHICULO
    CASE
      WHEN COUNTIF(
        Zona_Entrega IS NOT NULL
        AND Zona_Entrega != "Urbana"
      ) > 0
      THEN "Lejana"
      ELSE "Urbana"
    END AS Zona_Entrega,
    --- AQUI DETERMINAMOS SI EL VEHICULO REQUIRIÓ UN ESTIBADOR O NO
    MAX(
      CASE
        WHEN LOWER(visit__notes) LIKE '%esti%' THEN 1
        ELSE 0
      END
    ) AS estiba_flag,
    ARRAY_AGG(
      STRUCT(
        visit__id AS visit_id,
        Num_reserva AS num_reserva,
        Zona_Entrega AS zona_entrega,
        tarifario_final AS tarifa_reserva,
        visit__status AS estado_reserva
      )
    ) AS Atributo_reservas

  FROM base_reservas_visitas_enriquecida
  GROUP BY
    fecha_despacho,
    vehiculo,
    cc_despacha
),

base_camion_2 AS(
SELECT
  *,
  -- ✅ Texto después del último guion
  TRIM(
    REGEXP_EXTRACT(vehiculo, r'.*-(.*)$')
  ) AS capacidad_raw,

  -- ✅ Tipo de capacidad normalizada
  CASE
  
  -- ✅ REGLA DURA DE NEGOCIO (manda sobre el texto)
  WHEN SAFE_CAST(
         REGEXP_EXTRACT(
           REGEXP_EXTRACT(vehiculo, r'.*-(.*)$'),
           r'(\d+(?:\.\d+)?)'
         ) AS INT64
       ) IN (13, 20, 30)
    THEN 'METROS_CUBICOS'

    WHEN REGEXP_CONTAINS(
           REGEXP_EXTRACT(vehiculo, r'.*-(.*)$'),
           r'(?i)TN|T$'
         )
      THEN 'TONELADAS'
    WHEN REGEXP_CONTAINS(
           REGEXP_EXTRACT(vehiculo, r'.*-(.*)$'),
           r'(?i)M'
         )
      THEN 'METROS_CUBICOS'
    ELSE 'DESCONOCIDO'
  END AS tipo_capacidad,
  -- ✅ Valor numérico limpio (SIN error)
  SAFE_CAST(
    REGEXP_EXTRACT(
      REGEXP_EXTRACT(vehiculo, r'.*-(.*)$'),
      r'(\d+(?:\.\d+)?)'
    ) AS FLOAT64
  ) AS capacidad_valor
FROM base_camion
),

base_camion_3 AS (
  SELECT
  *,
    -- AQUÍ DEFINIMOS EL TIPO DE VEHÍCULO SEGÚN SU CAPACIDAD
  -- VEHICULOS POR TONELAJE
  CASE
    WHEN tipo_capacidad = "TONELADAS" AND capacidad_valor <= 3 THEN "CAMIÓN 3TN"
    WHEN tipo_capacidad = "TONELADAS" AND capacidad_valor <= 5 THEN "CAMIÓN 5TN"
    WHEN tipo_capacidad = "TONELADAS" AND capacidad_valor <= 8 THEN "CAMIÓN 8TN"
    WHEN tipo_capacidad = "TONELADAS" THEN "CAMIÓN PESADO"
  -- VEHICULOS POR vOLUMEN
    WHEN capacidad_valor =13 THEN "CAMIÓN 13M3"
    WHEN capacidad_valor =20 THEN "CAMIÓN 20M3"
    WHEN capacidad_valor =30 THEN "CAMIÓN 30M3"
    WHEN tipo_capacidad = "METROS_CUBICOS" THEN "CAMIÓN POR VOLUMEN"
    ELSE "NO CLASIFICADO"
  END AS tipo_vehiculo
  FROM base_camion_2
),

---------------------------------------------------------------------------------------------------------
---AQUI VAMOS A CONTRUIR LOS TARIFARIOS - AQUI SE PUEDEN ACTUALIZAR LOS COSTOS SEGÚN CAMBIOS DEL PROVEEDOR
----------------------------------------------------------------------------------------------------------
---TARIFARIO POR TONELADAS
tarifario_tn AS (
  SELECT '3TN' AS capacidad, 400 AS urb, 530 AS lej, 540 AS est_urb, 615 AS est_lej UNION ALL
  SELECT '5TN', 560, 730, 635, 795 UNION ALL
  SELECT '8TN', 660, 870, 750, 940 UNION ALL
  SELECT '10TN',700, 950, 790,1000
),
---TARIFARIO POR METROS CUBICOS
tarifario_m3 AS (
  SELECT '13M3' AS capacidad, 425 AS urb, 615 AS lej UNION ALL
  SELECT '20M3', 480, 732 UNION ALL
  SELECT '30M3', 600, 820
),
costo_vehiculo AS (
  SELECT
    t.*,

    CASE
      -- 🚚 TONELADAS
      WHEN tipo_capacidad = 'TONELADAS' THEN
        CASE
          WHEN estiba_flag = 1 AND Zona_Entrega = 'Urbana' THEN tn.est_urb
          WHEN estiba_flag = 1 AND Zona_Entrega != 'Urbana' THEN tn.est_lej
          WHEN estiba_flag = 0 AND Zona_Entrega = 'Urbana' THEN tn.urb
          ELSE tn.lej
        END

      -- 📦 METROS CÚBICOS
      WHEN tipo_capacidad = 'METROS_CUBICOS' THEN
        CASE
          WHEN Zona_Entrega = 'Urbana' THEN m3.urb
          ELSE m3.lej
        END

      ELSE NULL
    END AS costo_camion

  FROM (
    -- 🔑 Normalizamos la capacidad
    SELECT
      bc.*,
      CASE
        WHEN tipo_capacidad = 'TONELADAS'
          THEN CONCAT(capacidad_valor, 'TN')
        WHEN tipo_capacidad = 'METROS_CUBICOS'
          THEN CONCAT(capacidad_valor, 'M3')
        ELSE NULL
      END AS capacidad_key
    FROM base_camion_3 bc
  ) t

  LEFT JOIN tarifario_tn tn
    ON t.capacidad_key = tn.capacidad

  LEFT JOIN tarifario_m3 m3
    ON t.capacidad_key = m3.capacidad
)

SELECT * FROM costo_vehiculo
WHERE reservas_cargadas!=1;
