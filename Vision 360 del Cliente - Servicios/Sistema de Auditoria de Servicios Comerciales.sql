/*
========================================================================================
PROYECTO: Sodtrack Intelligence Engine (Ecosistema Modelo Galaxia)
AUTOR: Sebastián Santos | Ingeniería Industrial (UNI)
OBJETIVO: Centralizar y reconstruir el ciclo de vida de servicios técnicos, 
          integrando métricas de presupuesto, ejecución operativa y CX.

HITOS TÉCNICOS RESUELTOS:
1. NORMALIZACIÓN MULTI-FUENTE: Uso de REGEXP_EXTRACT para estandarizar 'referencias' 
   sucias (Boletas/Facturas/JSON) en claves canónicas de 12 dígitos para cruce con Medallia.
2. MODULACIÓN DE EVENTOS: Reconstrucción de la historia de la reserva (primer 'created' 
   y último 'done') mediante Window Functions para evitar duplicidad.
3. ANALÍTICA PRESCRIPTIVA: Lógica para determinar el 'Estado del Proyecto' 
   (Presupuestado vs. Ejecutado) a nivel de Proyecto ID.
========================================================================================
*/

WITH bl_last AS (
  SELECT
    booking_id,
    TRIM(LOWER(new_status)) AS new_status,
    updated_date,
    user_id,
    ROW_NUMBER() OVER (
      PARTITION BY booking_id
      ORDER BY updated_date DESC, id DESC
    ) AS rn
  FROM `sodone.trf_pe.sodtrack_booking_log`
),
inc_dedup AS (
  SELECT
    CAST(entity_id AS STRING) AS booking_id_str,
    id,
    description,
    booking_incident_reason_id,
    booking_incident_status_id
  FROM `sodone.trf_pe.sodtrack_booking_incident`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CAST(entity_id AS STRING)
    ORDER BY id DESC  -- usa DESC o ASC según tu criterio
  ) = 1
)
,
bl_created AS (
  SELECT
    booking_id,
    user_id,
    new_status,
    updated_date,
    ROW_NUMBER() OVER (
      PARTITION BY booking_id
      ORDER BY updated_date ASC   -- primer 'created'
    ) AS rn
  FROM `sodone.trf_pe.sodtrack_booking_log`
  WHERE LOWER(new_status) = 'created'
),
ultima_boleta AS (
  SELECT *
  FROM (
    SELECT
      t.*,
      ROW_NUMBER() OVER (
        PARTITION BY shopping_cart_id
        ORDER BY id DESC
      ) AS rn
    FROM `sodone.trf_pe.sodtrack_external_transaction` AS t
  )
  WHERE rn = 1
),

---INTRODUCIMOS AQUI LAS VENTANAS PARA CALCULAR EL PRESUPUESTO, INCORPORACIÓN DEL BUDGET_EXECUTION
presupuesto AS (SELECT
  pre.*,
  SUM(COALESCE(CAST(pre.total_quote AS NUMERIC),0)) OVER(PARTITION BY budget_booking_id) AS costo_total
  FROM `sodone.trf_pe.sodtrack_budget_execution` AS pre
),

---TRAS REALIZAR EL ÇÁLCULO DE LOS PRESUPUESTOS INCORPORAMOS EL NUEVO BUDGET_EXECUTION
budget_execution AS (
  SELECT exe.id,
      exe.created_date,
      exe.name,
        CASE 
          WHEN exe.status = "provider_submitted" THEN "Enviado por el Proveedor"
          WHEN exe.status = "created" THEN "Cargado por el Proveedor"
          WHEN exe.status = "waiting_customer_approval" THEN "Esperando aprobación del Cliente"
          WHEN exe.status = "waiting_admin_approval" THEN "Esperando aprobación del Administrador"
          WHEN exe.status = "in_progress" THEN "En proceso de creación"
          WHEN exe.status = "customer_approved" THEN "Aprobado por el Cliente"
          WHEN exe.status = "customer_rejected" THEN "Rechazado por el Cliente"
          ELSE exe.status
        END AS Estado_presupuesto,
      exe.budget_booking_id,
      exe.costo_total
FROM presupuesto AS exe
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY exe.budget_booking_id  
    ORDER BY exe.created_date DESC
  ) = 1
),

-------------------------------------------------------------------------------------------------------------------------------------
------SECCIÓN 2: NORMALIZACIÓN DE LLAVES MEDIANTE FUNCIONES DE VENTANA CTE
------------------------------------------------------------------------------------------------------------------------------------

-- 2) Normalización del campo reference (igual que tu lógica para Medallia)
ref_norm AS (
  SELECT
    ub.*,
    reference AS raw,
    TRIM(reference) AS raw_trim,
    UPPER(TRIM(reference)) AS up,

    -- Tipo detectado B/F con evidencia clara para evitar falsos positivos
    CASE
      WHEN REGEXP_CONTAINS(UPPER(TRIM(reference)), r'\bFACTURA\b')
        OR REGEXP_CONTAINS(UPPER(TRIM(reference)), r'\bF\d+')
      THEN 'F'
      WHEN REGEXP_CONTAINS(UPPER(TRIM(reference)), r'\bBOLETA\b')
        OR REGEXP_CONTAINS(UPPER(TRIM(reference)), r'\bBLT\b')
        OR REGEXP_CONTAINS(UPPER(TRIM(reference)), r'\bB\d+')
      THEN 'B'
      ELSE NULL
    END AS tipo_detectado,

    -- Extracción del número (solo dígitos)
    COALESCE(
      -- Formato JSON tipo {"BLT":"000946368252"} o {"F":"000..."}
      REGEXP_EXTRACT(TRIM(reference), r'"[A-Za-z]*"\s*:\s*"(\d+)"'),
      -- Bloques largos de dígitos (>=6) en cualquier posición
      REGEXP_EXTRACT(TRIM(reference), r'(\d{6,})'),
      -- Dígitos al final del texto
      REGEXP_EXTRACT(TRIM(reference), r'(\d+)$')
    ) AS numero_extraido
  FROM ultima_boleta AS ub
),

-- 3) Campos normalizados y clave canónica
ref_final AS (
  SELECT
    r.*,

    -- Número normalizado SOLO para B/F (12 dígitos con padding)
    CASE
      WHEN r.tipo_detectado IN ('B','F') AND r.numero_extraido IS NOT NULL THEN
        CASE
          WHEN LENGTH(r.numero_extraido) >= 12 THEN r.numero_extraido
          ELSE LPAD(r.numero_extraido, 12, '0')
        END
      ELSE NULL
    END AS numero_norm_bf,

    -- Código final: B/F → 12 dígitos; si no es B/F → texto original limpio
    CASE
      WHEN r.tipo_detectado IN ('B','F') AND r.numero_extraido IS NOT NULL THEN
        CASE
          WHEN LENGTH(r.numero_extraido) >= 12 THEN r.numero_extraido
          ELSE LPAD(r.numero_extraido, 12, '0')
        END
      WHEN r.raw_trim IS NOT NULL AND r.raw_trim != '' THEN r.raw_trim
      ELSE NULL
    END AS codigo_final,

    -- Clave canónica para cruzar
    CASE
      WHEN r.tipo_detectado = 'B' AND r.numero_extraido IS NOT NULL THEN CONCAT('B-',
        CASE WHEN LENGTH(r.numero_extraido) >= 12 THEN r.numero_extraido ELSE LPAD(r.numero_extraido, 12, '0') END)
      WHEN r.tipo_detectado = 'F' AND r.numero_extraido IS NOT NULL THEN CONCAT('F-',
        CASE WHEN LENGTH(r.numero_extraido) >= 12 THEN r.numero_extraido ELSE LPAD(r.numero_extraido, 12, '0') END)
      ELSE r.raw_trim
    END AS clave_canonica_reference,

    -- Bandera de calidad opcional (útil para monitoreo)
    CASE
      WHEN r.tipo_detectado IN ('B','F') AND r.numero_extraido IS NULL THEN 'SIN_NUMERO_BF'
      WHEN (r.tipo_detectado IS NULL) AND (r.raw_trim IS NULL OR r.raw_trim = '') THEN 'VACIO'
      ELSE 'OK'
    END AS calidad_reference
  FROM ref_norm AS r
),
experiencia_instalaciones AS (
  SELECT
    a_surveyid,
    e_creationdate,
    e_responsedate,
    e_sdm_transaction_number_text,
    CAST(q_sdm_instalaciones_ltr AS INT64) - 1 AS nps,
    q_sdm_instalaciones_amabilidad_sat5 AS csat_instalaciones_amabilidad,
    q_sdm_instalaciones_calidad_sat5 AS csat_instalaciones_calidad,
    q_sdm_instalaciones_fechas_sat5 AS csat_instalaciones_fechas,
    q_sdm_instalaciones_orden_limpieza_sat5 AS csat_instalaciones_orden_limpieza,
    q_sdm_instalaciones_comment,
    q_sdm_instalaciones_comment_2,
    a_topics_sentiments_tagged_original
  FROM `sodone.acc.medallia_feedbacks_stores`
  WHERE e_sdm_instalaciones_proveedor_servicio IS NOT NULL
    AND e_sdm_country_alt = "4"
    AND EXTRACT(YEAR FROM e_responsedate) >= 2025
),

medallia_norm AS (
  SELECT
    expe.*,
    e_sdm_transaction_number_text AS raw,
    -- Normaliza espacios y nulos
    TRIM(e_sdm_transaction_number_text) AS raw_trim,
    UPPER(TRIM(e_sdm_transaction_number_text)) AS up,
    -- Detecta tipo SOLO si hay patrones claros (evitamos falsos positivos)
    CASE
      WHEN REGEXP_CONTAINS(UPPER(TRIM(e_sdm_transaction_number_text)), r'\bFACTURA\b') OR
           REGEXP_CONTAINS(UPPER(TRIM(e_sdm_transaction_number_text)), r'\bF\d+')        THEN 'F'
      WHEN REGEXP_CONTAINS(UPPER(TRIM(e_sdm_transaction_number_text)), r'\bBOLETA\b') OR
           REGEXP_CONTAINS(UPPER(TRIM(e_sdm_transaction_number_text)), r'\bBLT\b') OR
           REGEXP_CONTAINS(UPPER(TRIM(e_sdm_transaction_number_text)), r'\bB\d+')        THEN 'B'
      ELSE NULL
    END AS tipo_detectado,
    -- Extractores de dígitos para B/F
    COALESCE(
      -- JSON tipo {"BLT":"000946368252"} o {"F":"000..."}
      REGEXP_EXTRACT(TRIM(e_sdm_transaction_number_text), r'"[A-Za-z]*"\s*:\s*"(\d+)"'),
      -- Bloques largos de dígitos en cualquier posición
      REGEXP_EXTRACT(TRIM(e_sdm_transaction_number_text), r'(\d{6,})'),
      -- Dígitos al final (ej. “B: 000123”, “F000123”, etc.)
      REGEXP_EXTRACT(TRIM(e_sdm_transaction_number_text), r'(\d+)$')
    ) AS numero_extraido
  FROM experiencia_instalaciones AS expe
),

medallia_final AS (
  SELECT
    e.*,
    raw AS e_sdm_transaction_number_text,
    tipo_detectado,
    -- Número normalizado SOLO para B/F
    CASE
      WHEN tipo_detectado IN ('B','F') AND numero_extraido IS NOT NULL THEN
        CASE
          WHEN LENGTH(numero_extraido) >= 12 THEN numero_extraido
          ELSE LPAD(numero_extraido, 12, '0')
        END
      ELSE NULL
    END AS numero_norm_bf,
    -- Código final:
    -- - Si B/F → usamos número_norm_bf (12 dígitos).
    -- - Si NO B/F → se mantiene el texto original (raw_trim).
    CASE
      WHEN tipo_detectado IN ('B','F') AND numero_extraido IS NOT NULL THEN
        CASE
          WHEN LENGTH(numero_extraido) >= 12 THEN numero_extraido
          ELSE LPAD(numero_extraido, 12, '0')
        END
      WHEN raw_trim IS NOT NULL AND raw_trim != '' THEN raw_trim
      ELSE NULL
    END AS codigo_final,
    -- Clave canónica para cruce:
    CASE
      WHEN tipo_detectado = 'B' AND numero_extraido IS NOT NULL THEN CONCAT('B-', 
        CASE WHEN LENGTH(numero_extraido) >= 12 THEN numero_extraido ELSE LPAD(numero_extraido, 12, '0') END)
      WHEN tipo_detectado = 'F' AND numero_extraido IS NOT NULL THEN CONCAT('F-', 
        CASE WHEN LENGTH(numero_extraido) >= 12 THEN numero_extraido ELSE LPAD(numero_extraido, 12, '0') END)
      ELSE raw_trim  -- Mantener tal cual si no es B/F
    END AS clave_canonica,
    -- Bandera de calidad
  FROM medallia_norm AS e
),

base_booking AS(
SELECT
  -- Identificadores básicos
  a.id AS booking_id,
  a.professional_id,
  a.shopping_cart_id,

  -- Fechas del log (último done y primer created)
  bc.updated_date AS fecha_creacion,
  a.done_date AS fecha_finalizacion,
  a.date AS fecha_programacion,

  -- Estado del Clente
  CASE a.payment_status
    WHEN "pending" THEN "Pendiente"
    WHEN "approved" THEN "Aprobado"
  END AS estado_de_pago,
  CASE b.new_status 
    WHEN "cancelled" THEN "Cancelado"
    WHEN "done" THEN "Completado"
    WHEN "waiting_scheduling_mechanism" THEN "Esperando mecanismo de programación"
    WHEN "created" THEN "Creado"
    WHEN "waiting_customer_to_set_date" THEN "Esperando que el cliente defina la fecha"
    WHEN "arrived_to_destination" THEN "Llegó al destino"
    WHEN "accepted" THEN "Aceptado"
    WHEN "left_destination" THEN "Salió del destino"
    WHEN "waiting_assignment_mechanism" THEN "Esperando mecanismo de asignación"
    WHEN "waiting_provider_to_propose_date" THEN "Esperando que el proveedor proponga fecha"
    WHEN "on_my_way" THEN "En camino"
    WHEN "searching" THEN "Buscando"
    WHEN "waiting_customer_to_accept_proposed_date" THEN "Esperando que el cliente acepte la fecha propuesta"
  END AS estado_del_servicio,
  category.name AS categoria_servicio,
  type.name AS bucket_servicio,
  operation.name AS servicio_solicitado,
  service.name AS tipo_servicio,
  a.service_name AS nombre_servicio,
  COALESCE(REGEXP_EXTRACT(category.name,r'^([^ -]+)\s*-'), category.name) AS categoria_macroservicio,
-- Canal de Venta
  sale.name AS canal_de_venta,
  CAST(TRIM(SPLIT(sale.name, '-')[OFFSET(0)]) AS INT64) AS codigo_tienda,
  TRIM(SPLIT(sale.name, '-')[OFFSET(1)]) AS nombre_tienda,


  -- Incidentes
categoria.name AS categoria_incidente,
reason.name AS razon_incidente,
reason.description AS descripcion_incidente,

  -- Emails (cliente, profesional, creador)
  f.email        AS cliente_email,         -- desde shopping_cart.user_id
  proveedores.email        AS profesional_email,     -- desde professional.user_id
  u_creator.email AS creador_email,        -- desde booking_log(user_id) del primer 'created'

  --- Datos de Proveedores
COALESCE(
  NULLIF(TRIM(CONCAT(padre_professional_usuario.name, ' ', padre_professional_usuario.lastname)), ''),
  NULLIF(TRIM(CONCAT(proveedores.name, ' ', proveedores.lastname)), '')
) AS nombre_contratista,
  
  ge.gestion,
  CASE
    WHEN ROW_NUMBER()OVER(
      PARTITION BY medallia.a_surveyid
      ORDER BY a.id
    ) = 1 THEN "PRINCIPAL"
    ELSE "SECUNDARIO"
    END AS estado_encuesta,

--- DATOS DE EXPERIENCIA
  transaccion.clave_canonica_reference,
  medallia.e_creationdate,
  medallia.e_responsedate,
  COALESCE(CAST(medallia.e_responsedate AS DATE), CAST(a.date AS DATE)) AS fecha_respuesta,
  medallia.nps,
  medallia.csat_instalaciones_amabilidad,
  medallia.csat_instalaciones_calidad,
  medallia.csat_instalaciones_fechas,
  medallia.csat_instalaciones_orden_limpieza,
  medallia.q_sdm_instalaciones_comment AS comentario_principal,
  medallia.q_sdm_instalaciones_comment_2 AS comentario_secundario,
  medallia.a_topics_sentiments_tagged_original AS topicos_medallia,

  -- (Opcional) IDs de usuarios
  CAST(e.user_id AS INT64)     AS cliente_user_id,
  CAST(c.user_id AS INT64)     AS profesional_user_id,
  CAST(bc.user_id AS INT64)    AS creador_user_id,

  ---- Precios y Costos
  CAST(a.final_cost AS FLOAT64) AS costo,
  CAST(a.final_price AS FLOAT64) AS importe,

  ---ID DEL PROYECTO PADRE
  e.project_id AS Proyecto_id,

  ---- Presupuestos sin ejecutar
  CASE
  WHEN variant.is_budget = true THEN "Presupuestado"
  ELSE "No Presupuestado"
END AS Presupuesto_flag,
presupuesto.costo_total AS costo_presupuesto

  -- Se puede añadir más campos de a/c/e/f según necesites
FROM `sodone.trf_pe.sodtrack_booking` AS a

-- Último 'done'
LEFT JOIN bl_last AS b
  ON CAST(a.id AS INT64) = b.booking_id
 AND b.rn = 1

-- Primer 'created' (creador de la reserva)
LEFT JOIN bl_created AS bc
  ON CAST(a.id AS INT64) = bc.booking_id
 AND bc.rn = 1

-- Profesional
LEFT JOIN `sodone.trf_pe.sodtrack_professional` AS c
  ON CAST(a.professional_id AS INT64) = CAST(c.id AS INT64)
LEFT JOIN `sodfunsx.experiencia_pe.Usuarios_sodtrack` AS proveedores
  ON CAST(c.user_id AS INT64) = CAST(proveedores.id AS INT64)

-- Cliente (desde el carrito)
LEFT JOIN `sodone.trf_pe.sodtrack_shopping_cart` AS e
  ON CAST(a.shopping_cart_id AS INT64) = CAST(e.id AS INT64)
LEFT JOIN `sodfunsx.experiencia_pe.Usuarios_sodtrack` AS f
  ON CAST(e.user_id AS INT64) = CAST(f.id AS INT64)

-- Usuario creador (actor del primer 'created')
LEFT JOIN `sodfunsx.experiencia_pe.Usuarios_sodtrack` AS u_creator
  ON CAST(b.user_id AS INT64) = CAST(u_creator.id AS INT64)

---- Tabla de Gestión 
LEFT JOIN `sodfunsx.experiencia_pe.Gestion_Sodtrack` AS ge
  ON u_creator.email = ge.Correo_creador
LEFT JOIN inc_dedup AS inc
  ON inc.booking_id_str = a.id
LEFT JOIN `sodone.trf_pe.sodtrack_booking_incident_reason` AS reason
  ON inc.booking_incident_reason_id = reason.id
LEFT JOIN `sodfunsx.experiencia_pe.Sodtrack_categoria_incidentes` AS categoria
  ON reason.booking_incident_category_id = categoria.id
LEFT JOIN `sodone.trf_pe.sodtrack_variant_operation` AS operation
  ON a.service_variant_area_id = CAST(operation.id AS STRING)
LEFT JOIN `sodone.trf_pe.sodtrack_service_variant` AS variant
  ON operation.service_variant_id = variant.id
LEFT JOIN `sodone.trf_pe.sodtrack_service` AS service
  ON variant.service_id = service.id
LEFT JOIN `sodone.trf_pe.sodtrack_service_type` AS type
  ON service.service_type_id = type.id
LEFT JOIN `sodone.trf_pe.sodtrack_category` AS category
  ON a.category_id = CAST(category.id AS STRING)
LEFT JOIN `sodone.trf_pe.sodtrack_sale_channel` AS sale
  ON CAST(a.sale_channel_id AS INT64) = sale.id
---- Tabla Proveedores e email
LEFT JOIN `sodone.trf_pe.sodtrack_professional` AS padre_professional
  ON CAST(c.parent_id AS INT64) = padre_professional.id
LEFT JOIN `sodfunsx.experiencia_pe.Usuarios_sodtrack` AS padre_professional_usuario
  ON CAST(padre_professional.user_id AS INT64) = CAST(padre_professional_usuario.id AS INT64)
---CRUCE CON BOLETAS
LEFT JOIN ref_final AS transaccion
  ON transaccion.shopping_cart_id = e.id
---CRUCE CON EL PRESUPUESTO DEL NUEVO BUDGET EXECUTION
LEFT JOIN budget_execution AS presupuesto
  ON a.id = CAST(presupuesto.budget_booking_id AS STRING)
--- CRUCE CON MEDALLIA
LEFT JOIN medallia_final AS medallia
  ON transaccion.clave_canonica_reference = medallia.clave_canonica
),
proyecto_tracking AS (
  SELECT
    ba.*,
    -- ¿Hubo presupuesto en el proyecto?
    MAX(
      CASE WHEN Presupuesto_flag = 'Presupuestado' THEN 1 ELSE 0 END
    ) OVER (PARTITION BY Proyecto_id) AS proyecto_tiene_presupuesto,

    -- ¿Hubo ejecución en el proyecto?
    MAX(
      CASE 
        WHEN estado_del_servicio = 'Completado'
             AND Presupuesto_flag = 'No Presupuestado'
        THEN 1 ELSE 0 
      END
    ) OVER (PARTITION BY Proyecto_id) AS proyecto_tiene_ejecucion
  FROM base_booking as ba
)
SELECT
  b.*,
  -- Rol del booking dentro del proyecto
  CASE
    WHEN b.Presupuesto_flag = 'Presupuestado'
      THEN 'PRESUPUESTO'
    WHEN b.estado_del_servicio = 'Completado'
         AND b.Presupuesto_flag = 'No Presupuestado'
      THEN 'EJECUCION'
    ELSE 'OTRO'
  END AS rol_booking_en_proyecto,

  -- Estado final del proyecto
  CASE
    WHEN b.proyecto_tiene_presupuesto = 1
         AND b.proyecto_tiene_ejecucion = 1
      THEN 'EJECUTADO'
    WHEN b.proyecto_tiene_presupuesto = 1
         AND b.proyecto_tiene_ejecucion = 0
      THEN 'PENDIENTE DE EJECUTAR'
    ELSE 'SIN PRESUPUESTO'
  END AS estado_proyecto
FROM proyecto_tracking b;

