/*
===============================================================================
Pipeline de Transformación (SQL) - Lead Time y Cobertura de Despacho
===============================================================================
Descripción:
Este script toma la data cruda cargada desde Oracle, la enriquece con maestros 
geográficos y calcula dinámicamente los días reales de despacho aplicando el 
Lead Time (LT) de cada Centro de Costo. Finalmente, proyecta la fecha mínima 
de entrega.

ÍNDICE DEL SCRIPT:
------------------
    1. CTE `dz`: Cruce de la configuración de zonas con el maestro geográfico.
    2. CTE `mapping`: Creación de un índice numérico para los días de la semana.
    3. CTE `base`: Transformación core. Desplazamiento de días de entrega usando 
       expresiones regulares y aritmética modular. Integración de Ubigeos.
    4. CONSULTA FINAL: Generación de un array a 14 días para encontrar la 
       fecha exacta del próximo despacho disponible.
===============================================================================
*/

-- 1. ENRIQUECIMIENTO GEOGRÁFICO BASE
WITH dz AS (
  SELECT 
    dz.*, 
    lk.* EXCEPT (COMUNA)
  FROM `sod-pe-bi-sandbox.Omnicanal_Pe.CC_distrito_zona` dz
  LEFT JOIN `sodfunsx.abastecimiento_pe.LK_DIVGEO_2` lk
    ON dz.COMUNA = lk.COMUNA
),

-- 2. MAPEO DE DÍAS DE LA SEMANA A ÍNDICES (L=1, M=2, ..., D=7)
mapping AS (
  SELECT 'L' AS letra, 1 AS idx UNION ALL
  SELECT 'M', 2 UNION ALL
  SELECT 'W', 3 UNION ALL
  SELECT 'J', 4 UNION ALL
  SELECT 'V', 5 UNION ALL
  SELECT 'S', 6 UNION ALL
  SELECT 'D', 7
),

-- 3. MOTOR DE REGLAS Y DESPLAZAMIENTO DE DÍAS (CORE LOGIC)
base AS (
  SELECT
    A.CCOSTO,
    A.DESC_ZONA,
    A.DIAS_DESPZONA,
    A.TIEMPO_DESP,
    
    -- Lógica de desplazamiento: Lee el string de días (ej. LMW), extrae cada letra, 
    -- busca su índice, le suma el Lead Time (TIEMPO_DESP) y usa MOD para reasignar 
    -- el nuevo día de la semana.
    CASE 
      WHEN A.DIAS_DESPZONA IS NULL OR A.DIAS_DESPZONA = '' THEN NULL
      ELSE (
        SELECT STRING_AGG(nuevo_dia, '' ORDER BY idx_nuevo)
        FROM (
          SELECT DISTINCT
            m2.letra AS nuevo_dia,
            m2.idx AS idx_nuevo
          FROM UNNEST(
                REGEXP_EXTRACT_ALL(
                  REPLACE(REPLACE(A.DIAS_DESPZONA, '-', ''), ' ', ''),
                  r'.'
                )
              ) AS c
          JOIN mapping m1 ON m1.letra = c
          JOIN mapping m2
            ON m2.idx = MOD(m1.idx - 1 + IFNULL(CAST(A.TIEMPO_DESP AS INT64), 0), 7) + 1
        )
      )
    END AS DIAS_DESPZONA_REAL,
    
    A.COMUNA,
    A.CIUDAD,
    A.REGION,
    B.CCOSTO AS CD_STS,
    B.TIEMPO_DESP AS TIEMPO_STS,
    C.TIENDA,
    C.TIENDA AS NOMBRE_TIENDA, -- Alias opcional para claridad
    C.TIEMPO_PROCESO,
    C.DD_HORA_CORTE,
    D.UBIGEO,
    D.UBICACION,
    D.POBLACION,
    D.REGION_REAL,
    D.DISTRITO_REAL,
    D.LATITUD,
    D.LONGITUD,
    D.COBERTURA_DD
    
  FROM `sodfunsx.abastecimiento_pe.Ubigeos_PE` D
  LEFT JOIN dz A
    ON A.zone_id = D.Ubigeo
   AND A.CANAL_VENTA = 33
   AND (CAST(A.CAPACIDAD AS INT64) <> 0 OR A.CAPACIDAD IS NULL)
  LEFT JOIN `sodfunsx.abastecimiento_pe.LT_STS` B
    ON A.CCOSTO = B.TIENDA
   AND B.DESACTIVADO = 0
  LEFT JOIN `sodfunsx.abastecimiento_pe.CC_GCP` C
    ON CAST(A.CCOSTO AS STRING) = C.CC
)

-- 4. PROYECCIÓN DE FECHA MÍNIMA DE DESPACHO
SELECT
  base.*,
  (
    -- Subconsulta que evalúa los próximos 14 días y selecciona la fecha más cercana 
    -- que coincida con los días de despacho configurados.
    SELECT
      DATE_ADD(
        fecha_base,
        INTERVAL IFNULL(CAST(base.TIEMPO_DESP AS INT64), 0) DAY
      )
    FROM (
      SELECT fecha_base
      FROM (
        SELECT
          DATE_ADD(CURRENT_DATE(), INTERVAL offset DAY) AS fecha_base,
          -- Ajuste del Day of Week nativo de BQ (Domingo=1) a formato estándar Lunes=1
          CASE
            WHEN EXTRACT(DAYOFWEEK FROM DATE_ADD(CURRENT_DATE(), INTERVAL offset DAY)) = 1 THEN 7
            ELSE EXTRACT(DAYOFWEEK FROM DATE_ADD(CURRENT_DATE(), INTERVAL offset DAY)) - 1
          END AS dow_norm
        FROM UNNEST(GENERATE_ARRAY(1, 14)) AS offset
      )
      WHERE dow_norm IN (
        SELECT m.idx
        FROM UNNEST(REGEXP_EXTRACT_ALL(
               REPLACE(REPLACE(base.DIAS_DESPZONA, '-', ''), ' ', ''),
               r'.'
             )) d
        JOIN mapping m ON m.letra = d
      )
      ORDER BY fecha_base
      LIMIT 1
    )
  ) AS FECHA_MIN_DESPACHO
FROM base;
