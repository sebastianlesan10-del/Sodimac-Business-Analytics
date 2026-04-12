/*******************************************************************************
PROYECTO: Ecosistema - Centro de Mando CX
AUTOR: Sebastián Santos
OBJETIVO: Normalización e integración de feedback Medallia con data transaccional.
FUENTES: medallia_feedbacks_stores, CES_OFICIAL, NPS_DINERO, transacciones_BI.
*******************************************************************************/
SELECT distinct
  dg.a_surveyid,
  dg.e_responsedate,
  EXTRACT(MONTH FROM dg.e_responsedate) AS mes,
  EXTRACT(MONTH FROM dg.e_sdm_transaction_date_date) AS mes_transaccion,
   CASE
    WHEN EXTRACT(MONTH FROM dg.e_sdm_transaction_date_date) = 1 THEN '1. Enero'
    WHEN EXTRACT(MONTH FROM dg.e_sdm_transaction_date_date) = 2 THEN '2. Febrero'
    WHEN EXTRACT(MONTH FROM dg.e_sdm_transaction_date_date) = 3 THEN '3. Marzo'
    WHEN EXTRACT(MONTH FROM dg.e_sdm_transaction_date_date) = 4 THEN '4. Abril'
    WHEN EXTRACT(MONTH FROM dg.e_sdm_transaction_date_date) = 5 THEN '5. Mayo'
    WHEN EXTRACT(MONTH FROM dg.e_sdm_transaction_date_date) = 6 THEN '6. Junio'
    WHEN EXTRACT(MONTH FROM dg.e_sdm_transaction_date_date) = 7 THEN '7. Julio'
    WHEN EXTRACT(MONTH FROM dg.e_sdm_transaction_date_date) = 8 THEN '8. Agosto'
    WHEN EXTRACT(MONTH FROM dg.e_sdm_transaction_date_date) = 9 THEN '9. Septiembre'
    WHEN EXTRACT(MONTH FROM dg.e_sdm_transaction_date_date) = 10 THEN '10. Octubre'
    WHEN EXTRACT(MONTH FROM dg.e_sdm_transaction_date_date) = 11 THEN '11. Noviembre'
    WHEN EXTRACT(MONTH FROM dg.e_sdm_transaction_date_date) = 12 THEN '12. Diciembre'
    ELSE NULL
  END AS mes_transaccion_nombre_numero,
  EXTRACT(YEAR FROM dg.e_responsedate) AS anio,
  EXTRACT(DAY FROM dg.e_sdm_transaction_date_date) AS cod_dia_trx,
  CONCAT(CAST(EXTRACT(MONTH FROM dg.e_responsedate) AS STRING), CAST(EXTRACT(YEAR FROM dg.e_responsedate) AS STRING)) AS mesanioconcatenados,
  dg.q_sdm_ltr11,
  dg.e_sdm_id_orden_venta,
  dg.e_sdm_country_alt,
  dg.q_sdm_tienda_facilidad_encontrar_productos_sat5,
  dg.e_sdm_transaction_number_text,
  dg.e_sdm_department_category_code_text,
  dg.q_sdm_tienda_info_entrega_producto_sat5,
  dg.e_sdm_unique_id,
  dg.e_sdm_department_category_text,
  dg.q_sdm_tienda_productos_variedad_sat5,
  dg.e_sdm_department_radio,
  dg.e_sdm_survey_type_alt,
  dg.q_sdm_ces_scale5 as Escala_NEC,
  dg.e_sdm_ces_yn as NEC,
  REGEXP_EXTRACT(SPLIT(dg.a_topics_sentiments_tagged_original, ', ')[SAFE_OFFSET(0)], r'^(.*?) \(') AS topico_1,
  REGEXP_EXTRACT(SPLIT(dg.a_topics_sentiments_tagged_original, ', ')[SAFE_OFFSET(0)], r'\((.*?)\)$') AS sentimento_1,

  REGEXP_EXTRACT(SPLIT(dg.a_topics_sentiments_tagged_original, ', ')[SAFE_OFFSET(1)], r'^(.*?) \(') AS topico_2,
  REGEXP_EXTRACT(SPLIT(dg.a_topics_sentiments_tagged_original, ', ')[SAFE_OFFSET(1)], r'\((.*?)\)$') AS sentimento_2,

  REGEXP_EXTRACT(SPLIT(dg.a_topics_sentiments_tagged_original, ', ')[SAFE_OFFSET(2)], r'^(.*?) \(') AS topico_3,
  REGEXP_EXTRACT(SPLIT(dg.a_topics_sentiments_tagged_original, ', ')[SAFE_OFFSET(2)], r'\((.*?)\)$') AS sentimento_3,
  dg.e_sdm_brand_alt,
    CASE 
    WHEN c.terminales is not null and c.cod is not null then "MPOS"
    WHEN LEFT(dg.e_sdm_transaction_number_text, 3) IN ("847", "838", "844", "598", "755", "784", "788", "831", "873", "958", "830", "888", "976", "977", "865", "912", "845", "712", "426", "854", "925", "568", "231", "883","849","840")
      OR LEFT(dg.e_sdm_transaction_number_text, 6) IN ("000847", "000838", "000844", "000598", "000755", "000784", "000788", "000831", "000873", "000958", "000830", "000888", "000976", "000977", "000865", "000912", "000845", "000712", "000426", "000854", "000925", "000568", "000231", "000883","000849","000840")
    THEN "Caja SCO"
    ELSE "Caja Asistida"
  END AS SCO_Trx,
  dg.e_unitid,
  REGEXP_EXTRACT(dg.e_unitid, r'-([0-9]+)$') AS unit_number,
  dg.e_sdm_hora_tirilla_ticket,
  SAFE_CAST(SUBSTR(dg.e_sdm_hora_tirilla_ticket, 0, 2) AS INT64) AS hora_extraida,

CASE
  WHEN SAFE_CAST(SUBSTR(dg.e_sdm_hora_tirilla_ticket, 0, 2) AS INT64) BETWEEN 7 AND 22 THEN
    CONCAT(
      CAST(SAFE_CAST(SUBSTR(dg.e_sdm_hora_tirilla_ticket, 0, 2) AS INT64) AS STRING), ':00 - ',
      CAST(SAFE_CAST(SUBSTR(dg.e_sdm_hora_tirilla_ticket, 0, 2) AS INT64) + 1 AS STRING), ':00'
    )
  WHEN dg.e_sdm_hora_tirilla_ticket IS NULL THEN NULL
  ELSE 'no identificado'
END AS rango_horario,
  dg.q_sdm_tienda_productos_cantidad_sat5,
  dg.e_sdm_punto_contacto,
  dg.q_sdm_tienda_productos_calidad_sat5,
  dg.e_sdm_store_name_text,
  CONCAT(dg.e_sdm_store_name_text, CAST(EXTRACT(MONTH FROM dg.e_responsedate) AS STRING), CAST(EXTRACT(YEAR FROM dg.e_responsedate) AS STRING)) AS llave_tienda_respuesta,
  dg.q_sdm_tienda_amabilidad_vendedores_sat5,
  dg.e_sdm_store_type_radio,  

CASE
  WHEN EXTRACT(YEAR FROM dg.e_responsedate) < 2024 OR (EXTRACT(YEAR FROM dg.e_responsedate) = 2024 AND EXTRACT(MONTH FROM dg.e_responsedate) < 6) THEN
    CASE
      WHEN dg.e_sdm_store_type_radio = '1' THEN 'TIENDA'
      WHEN dg.e_sdm_store_type_radio = '10' THEN 'DEVOLUCIONES'
      WHEN dg.e_sdm_store_type_radio = '8' THEN 'VENTA EMPRESA'
      ELSE 'NO IDENTIFICADO'
    END
  ELSE
    CASE
      WHEN dg.e_sdm_store_type_radio = '8' THEN 'VENTA EMPRESA'
      WHEN dg.e_sdm_store_type_radio IN ('2','3', '5', '11', '12', '15') OR dg.e_sdm_survey_type_alt IN ('6','9') THEN 'VAD'
      WHEN dg.e_sdm_store_type_radio in ('1') AND dg.e_sdm_survey_type_alt  in ('1', '7')  THEN 'TIENDA' 
      WHEN dg.e_sdm_survey_type_alt = '5' AND dg.e_sdm_punto_contacto = '9' AND (dg.e_sdm_store_type_radio = '1' OR dg.e_sdm_store_type_radio = '10') THEN 'DEVOLUCIONES'
      ELSE 'NO IDENTIFICADO'
    END
END AS canal_venta_general,

CONCAT(
  CASE
    WHEN EXTRACT(YEAR FROM dg.e_responsedate) < 2024 OR (EXTRACT(YEAR FROM dg.e_responsedate) = 2024 AND EXTRACT(MONTH FROM dg.e_responsedate) < 6) THEN
      CASE
        WHEN dg.e_sdm_store_type_radio = '1' THEN 'TIENDA'
        WHEN dg.e_sdm_store_type_radio = '10' THEN 'DEVOLUCIONES'
        WHEN dg.e_sdm_store_type_radio = '8' THEN 'VENTA EMPRESA'
        ELSE 'NO IDENTIFICADO'
      END
    ELSE
      CASE
        WHEN dg.e_sdm_store_type_radio = '8' THEN 'VENTA EMPRESA'
        WHEN dg.e_sdm_store_type_radio IN ('2','3', '5', '11', '12', '15') OR dg.e_sdm_survey_type_alt IN ('6','9') THEN 'VAD'
      WHEN dg.e_sdm_store_type_radio in ('1') AND dg.e_sdm_survey_type_alt  in ('1', '7')  THEN 'TIENDA' 
        WHEN dg.e_sdm_survey_type_alt = '5' AND dg.e_sdm_punto_contacto = '9' AND (dg.e_sdm_store_type_radio = '1' OR dg.e_sdm_store_type_radio = '10') THEN 'DEVOLUCIONES'
        ELSE 'NO IDENTIFICADO'
      END
  END,
  CONCAT(CAST(EXTRACT(MONTH FROM dg.e_responsedate) AS STRING), CAST(EXTRACT(YEAR FROM dg.e_responsedate) AS STRING))
) AS llave_canal_plan,

  dg.q_sdm_tienda_conocimiento_vendedores_sat5,
  FORMAT_DATE('%Y-%m-%d', DATE(dg.e_responsedate)) AS Fecha_actualizacion,
  dg.e_sdm_transaction_date_date,
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM dg.e_sdm_transaction_date_date) = 2 THEN '1. Lunes'
    WHEN EXTRACT(DAYOFWEEK FROM dg.e_sdm_transaction_date_date) = 3 THEN '2. Martes'
    WHEN EXTRACT(DAYOFWEEK FROM dg.e_sdm_transaction_date_date) = 4 THEN '3. Miércoles'
    WHEN EXTRACT(DAYOFWEEK FROM dg.e_sdm_transaction_date_date) = 5 THEN '4. Jueves'
    WHEN EXTRACT(DAYOFWEEK FROM dg.e_sdm_transaction_date_date) = 6 THEN '5. Viernes'
    WHEN EXTRACT(DAYOFWEEK FROM dg.e_sdm_transaction_date_date) = 7 THEN '6. Sábado'
    WHEN EXTRACT(DAYOFWEEK FROM dg.e_sdm_transaction_date_date) = 1 THEN '7. Domingo'
    ELSE NULL
  END AS dia_semana_ordenado,
  dg.q_sdm_tienda_disponibilidad_vendedores_sat5,
  dg.e_status,
  dg.q_sdm_ltr_follow_up_comment as comentario_principal,
  dg.q_sdm_home_improvement_feedback_comment as sugerencia_mejora,
  dg.q_sdm_tienda_facilidad_cotizaciones_sat5,
  dg.e_sdm_global_order_number,
  dg.q_sdm_home_product_use_alt,
  CASE
    WHEN EXTRACT(MONTH FROM dg.e_responsedate) = 1 THEN '1. Enero'
    WHEN EXTRACT(MONTH FROM dg.e_responsedate) = 2 THEN '2. Febrero'
    WHEN EXTRACT(MONTH FROM dg.e_responsedate) = 3 THEN '3. Marzo'
    WHEN EXTRACT(MONTH FROM dg.e_responsedate) = 4 THEN '4. Abril'
    WHEN EXTRACT(MONTH FROM dg.e_responsedate) = 5 THEN '5. Mayo'
    WHEN EXTRACT(MONTH FROM dg.e_responsedate) = 6 THEN '6. Junio'
    WHEN EXTRACT(MONTH FROM dg.e_responsedate) = 7 THEN '7. Julio'
    WHEN EXTRACT(MONTH FROM dg.e_responsedate) = 8 THEN '8. Agosto'
    WHEN EXTRACT(MONTH FROM dg.e_responsedate) = 9 THEN '9. Septiembre'
    WHEN EXTRACT(MONTH FROM dg.e_responsedate) = 10 THEN '10. Octubre'
    WHEN EXTRACT(MONTH FROM dg.e_responsedate) = 11 THEN '11. Noviembre'
    WHEN EXTRACT(MONTH FROM dg.e_responsedate) = 12 THEN '12. Diciembre'
    ELSE NULL
  END AS mes_nombre_numero,



 
CASE
    WHEN CAST(dg.q_sdm_ltr11 AS INT64) IN (10, 11) THEN 'Promotor'
    WHEN CAST(dg.q_sdm_ltr11 AS INT64) IN (8, 9) THEN 'Neutro'
    WHEN CAST(dg.q_sdm_ltr11 AS INT64) BETWEEN 1 AND 7 THEN 'Detractor'
    ELSE 'Sin Clasificación'
END AS categoria_nps,




  CASE
    WHEN CAST(dg.q_sdm_home_product_use_alt AS INT64) = 1 THEN 'Uso personal'
    WHEN CAST(dg.q_sdm_home_product_use_alt AS INT64) = 2 THEN 'Uso laboral'
    ELSE NULL
  END AS uso_producto,
  dg.q_sdm_ropo_enum,
  CASE
    WHEN CAST(dg.q_sdm_ropo_enum AS INT64) = 1 THEN 'Si'
    WHEN CAST(dg.q_sdm_ropo_enum AS INT64) = 2 THEN 'No'
    ELSE NULL
  END AS Ropo,
  dg.q_sdm_home_competitive_prices_alt,
  CASE
    WHEN dg.q_sdm_home_competitive_prices_alt = '1' THEN 'De precio bajo'
    WHEN dg.q_sdm_home_competitive_prices_alt = '2' THEN 'Del mismo precio'
    WHEN dg.q_sdm_home_competitive_prices_alt = '3' THEN 'De alto precio'
    WHEN dg.q_sdm_home_competitive_prices_alt = '4' THEN 'De muy alto precio'
    ELSE NULL
  END AS precios_competitivos,
  dg.q_sdm_tienda_facilidad_proceso_pago_sat5,
  dg.q_sdm_utilizar_app_apoyo_enum,
  dg.q_sdm_tienda_rapidez_pago_sat5,
  dg.q_sdm_tienda_amabilidad_cajeros_sat5,
  dg.q_sdm_despacho_domicilio_osat5,
  dg.q_sdm_despacho_disponibilidad_fechas_sat5,
  dg.q_sdm_despacho_costo_sat5,
  dg.q_sdm_despacho_claridad_comunicaciones_sat5,
  dg.q_sdm_despacho_facilidad_informacion_seguimiento_sat5,
  dg.q_sdm_despacho_exactitud_entrega_sat5,
  dg.q_sdm_despacho_satisfaccion_transportador_sat5,
  dg.q_sdm_despacho_caracteristicas_producto_sat5,
  dg.q_sdm_retiro_tienda_osat5,
  dg.q_sdm_retiro_disponibilidad_fechas_sat5,
  dg.q_sdm_retiro_claridad_comunicaciones_sat5,
  dg.q_sdm_retiro_facilidad_informacion_seguimiento_sat5,
  dg.q_sdm_retiro_cumplimiento_plazos_entrega_sat5,
  dg.q_sdm_retiro_caracteristicas_producto_sat5,
  dg.q_sdm_retiro_amabilidad_personal_sat5,
  dg.q_sdm_retiro_rapidez_entrega_sat5,
  dg.q_sdm_servicio_devoluciones_osat5,
  dg.q_sdm_cyd_comunicacion_clara_sat5,
  dg.q_sdm_cyd_alternativas_lugar_sat5,
  dg.q_sdm_cyd_facilidad_sat5,
  dg.q_sdm_cyd_rapidez_sat5,
  dg.q_sdm_cyd_amabilidad_sat5,
  dg.q_sdm_cyd_conocimiento_politicas_sat5,
  dg.q_sdm_hi_dev_servicio_osat5,
  dg.q_sdm_hi_dev_informacion_clara_sat5,
  dg.q_sdm_hi_dev_tiempo_espera_sat5,
  dg.q_sdm_hi_dev_comodidad_lugar_sat5,
  dg.q_sdm_hi_dev_conocimiento_asesor_sat5,
  dg.q_sdm_hi_dev_amabilidad_asesor_sat5,
  dg.q_sdm_hi_dev_alternativas_ofrecidas_sat5,
  dg.q_sdm_hi_dev_expectativas_servicio_sat5,
  dg.q_sdm_instalaciones_calidad_sat5,
  dg.q_sdm_instalaciones_fechas_sat5,
  dg.q_sdm_instalaciones_amabilidad_sat5,
  dg.q_sdm_instalaciones_orden_limpieza_sat5,
  dg.q_sdm_vtaemps_calidad_productos_sat5,
  dg.q_sdm_vtaemps_calidad_servicio_osat5,
  dg.q_sdm_vtaemps_cumplimiento_entrega_sat5,
  dg.q_sdm_vtaemps_disponibilidad_productos_sat5,
  dg.q_sdm_vtaemps_financiamiento_sat5,
  dg.q_sdm_vtaemps_opciones_entrega_sat5,
  dg.q_sdm_vtaemps_productos_precio_sat5,
  dg.q_sdm_vtaemps_rapidez_procesos_sat5,
  dg.q_sdm_vtaemps_variedad_productos_sat5,
  substr(dg.e_sdm_transaction_number_text,-9) as Cod_trx ,
  pt.NPS/10 AS NPS_PLAN_TIENDA,
  pt.LLAVE_TIENDA,
  info.TIENDA,
  info.REGION,
  info.SUBREGION,
  info.TIPO_TIENDA,
  info.GERENTE_REGIONAL,
  info.CATEGORIA,
  info.TIENDA_SCO,
  info.TRANSFORMACION,
  info.S1,
  info.S2,
  info.S3,
  info.S4,
  info.S5,
  pc.NPS_CIA / 10.0 AS NPS_CIA,
  CES.a_surveyid as surveyid_ces,
  CES.sales_margin_dt,
  CES.customer_ces_dt,
  CES.ces_level_val,
  aux.Cod,
  DIN.fecha_transaccion,
  DIN.VENTA,
  DIN.COSTO,
  DIN.UNIDADES,
  DIN.main_customer_id,
  DIN.cliente_volvio,
  DIN.dias_para_volver,
  DIN.veces_que_volvio,
  DIN.cliente_habia_ido,
  DIN.dias_desde_ultima_visita,
  DIN.veces_que_habia_ido,
  canal_plan.NPS AS plan_canal_nps,

--- Campos para Transacciones por NPS
bi.transactions3 AS Transacciones,
bi.unidades AS Unidades_vendidas,
bi.venta AS Ventas

FROM
  `sodone.acc.medallia_feedbacks_stores` dg
LEFT JOIN
  `sodfunsx.experiencia_pe.CES_OFICIAL_TABLA` CES
  ON dg.a_surveyid = CES.a_surveyid
LEFT JOIN
  `sodfunsx.experiencia_pe.NPS_DINERO` DIN
  ON dg.a_surveyid = DIN.id
-- Primero se une la información de tiendas para poder usar info.TIENDA en el siguiente JOIN
LEFT JOIN
  `sodfunsx.experiencia_pe.Informacion_tiendas` info
  ON REGEXP_EXTRACT(dg.e_unitid, r'-([0-9]+)$') = CAST(info.CODIGO AS STRING)
-- Luego se une el plan de tienda usando info.TIENDA
LEFT JOIN
  `sodfunsx.experiencia_pe.Planes_Tienda_Respuesta` pt
  ON CONCAT(
    CAST(EXTRACT(YEAR FROM dg.e_responsedate) AS STRING),
    FORMAT_DATE('%m', dg.e_responsedate),
    info.CODIGO) = pt.LLAVE_TIENDA
-- Unión con tabla auxiliar
LEFT JOIN
  `sodfunsx.experiencia_pe.Tabla_tienda_auxiliar` aux
  ON dg.e_sdm_store_name_text = aux.Nombre_Tienda
-- Unión con plan de compañía
LEFT JOIN
  `sodfunsx.experiencia_pe.Plan_Compañia` pc
  ON CONCAT('CIA', CAST(EXTRACT(MONTH FROM dg.e_responsedate) AS STRING), CAST(EXTRACT(YEAR FROM dg.e_responsedate) AS STRING)) = pc.      LLAVE_CIA
-- Unión con plan de canal
LEFT JOIN
  `sodfunsx.experiencia_pe.Planes_Canal` canal_plan
ON
  CONCAT(
    CASE
      WHEN dg.e_sdm_store_type_radio = '8' THEN 'VENTA EMPRESA'
      WHEN dg.e_sdm_store_type_radio IN ('2','3', '5', '11', '12', '15') OR dg.e_sdm_survey_type_alt IN ('6','9') THEN 'VAD'
      WHEN dg.e_sdm_store_type_radio = '1' AND dg.e_sdm_survey_type_alt = '1' THEN 'TIENDA'
      WHEN dg.e_sdm_survey_type_alt = '5' AND dg.e_sdm_punto_contacto = '9' AND (dg.e_sdm_store_type_radio = '1' OR dg.e_sdm_store_type_radio = '10') THEN 'DEVOLUCIONES'
      ELSE 'NO IDENTIFICADO'
    END,
    CONCAT(CAST(EXTRACT(MONTH FROM dg.e_responsedate) AS STRING), CAST(EXTRACT(YEAR FROM dg.e_responsedate) AS STRING))
  ) = canal_plan.LLAVE
LEFT JOIN `sod-pe-bi-sandbox.buc_sod_pe_bi.tbl_report_caja_ecom` AS b
  ON dg.e_sdm_transaction_number_text = b.num_documento
LEFT JOIN `sodfunsx.experiencia_pe.Terminales_mpos` AS c 
  ON c.Terminales = b.codigo_cajero AND c.Cod = b.sucursal
LEFT JOIN `sodfunsx.experiencia_pe.transacciones_BI` AS bi
  ON CONCAT(
    LPAD(CAST(info.CODIGO AS STRING), 3, "0"),
    LPAD(CAST(EXTRACT(MONTH FROM dg.e_responsedate) AS STRING),2,"0"),
    CAST(EXTRACT(YEAR FROM dg.e_responsedate) AS STRING)
    ) = CONCAT(
      LPAD(CAST(bi.location_id AS STRING),3,"0"),
      LPAD(CAST(bi.month AS STRING),2,"0"),
      CAST(bi.year AS STRING)
    )
-- Filtros
WHERE
  dg.e_sdm_country_alt = '4'
  AND dg.e_status = 'COMPLETED'
  AND EXTRACT(YEAR FROM dg.e_responsedate) IN (2024, 2025,2026)
