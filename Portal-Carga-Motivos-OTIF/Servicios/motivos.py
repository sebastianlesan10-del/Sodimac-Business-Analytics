#-------------------------------------------------------------------------------------------------
#--1. IMPORTAMOS LAS LIBRERIAS NECESARIAS.
#-------------------------------------------------------------------------------------------------
import uuid
import pandas as pd
from datetime import datetime
from google.cloud import bigquery



from pathlib import Path
import os
from dotenv import load_dotenv


#-------------------------------------------------------------------------------------------------
#--2. CONFIGURACIONES
#-------------------------------------------------------------------------------------------------

load_dotenv()




BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID")

BASE_DIR = Path(__file__).resolve().parent.parent

CREDENTIALS_PATH = (
    BASE_DIR /
    os.getenv("GOOGLE_CREDENTIALS_FILE")
)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(
    CREDENTIALS_PATH
)




client = bigquery.Client(
    project=BQ_PROJECT_ID
)


#-------------------------------------------------------------------------------------------------
#--3. CONSTRUCCIÓN DINÁMICA DE TABLAS.
#-------------------------------------------------------------------------------------------------

def tabla(nombre_tabla: str) -> str:

    return (
        f"{BQ_PROJECT_ID}."
        f"{BQ_DATASET_ID}."
        f"{nombre_tabla}"
    )

#-------------------------------------------------------------------------------------------------
#--4. TABLA TEMPORAL.
#-------------------------------------------------------------------------------------------------

def crear_tabla_temporal():

    sufijo = uuid.uuid4().hex[:8]

    return tabla(
        f"MOTIVO_STG_{sufijo}"
    )

#-------------------------------------------------------------------------------------------------
#--5. CARGA DATAFRAME.
#-------------------------------------------------------------------------------------------------

def cargar_dataframe(df, nombre_tabla):

    trabajo = client.load_table_from_dataframe(
        df,
        nombre_tabla
    )

    trabajo.result()

    return True


#-------------------------------------------------------------------------------------------------
#--7. VALIDAR COLUMNAS.
#-------------------------------------------------------------------------------------------------

COLUMNAS_REQUERIDAS = [
    "Reserva",
    "SKU",
    "Motivo",
    "Sub_Motivo",
    "Responsable"
]


def validar_columnas(df):

    faltantes = [

        columna

        for columna in COLUMNAS_REQUERIDAS

        if columna not in df.columns

    ]

    if faltantes:

        raise ValueError(
            f"Columnas faltantes: {faltantes}"
        )

    return True

#-------------------------------------------------------------------------------------------------
#--9. TABLA MAESTRA DE MOTIVOS
#-------------------------------------------------------------------------------------------------

def crear_tabla_motivos():

    query = f"""
    CREATE TABLE IF NOT EXISTS `{tabla("MOTIVO_MAESTRA")}`
    (
        Reserva STRING,
        SKU STRING,
        Motivo STRING,
        Sub_Motivo STRING,
        Responsable STRING,

        Fecha_Carga TIMESTAMP,
        Fecha_Actualizacion TIMESTAMP
    )

    CLUSTER BY Reserva, SKU
    """

    client.query(query).result()

#-------------------------------------------------------------------------------------------------
#--10. MERGE INCREMENTAL PARA LA TABLA MAESTRAS + TABLAS STG
#-------------------------------------------------------------------------------------------------

def merge_motivos(tabla_temporal):

    query = f"""

    MERGE `{tabla("MOTIVO_MAESTRA")}` T

    USING `{tabla_temporal}` S

    ON
        T.Reserva = CAST(S.Reserva AS STRING)
        AND
        T.SKU = CAST(S.SKU AS STRING)

    WHEN MATCHED THEN

        UPDATE SET

            Motivo = S.Motivo,
            Sub_Motivo = S.Sub_Motivo,
            Responsable = S.Responsable,
            Fecha_Actualizacion = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN

        INSERT (
            Reserva,
            SKU,
            Motivo,
            Sub_Motivo,
            Responsable,
            Fecha_Carga,
            Fecha_Actualizacion
        )

        VALUES (
            CAST(S.Reserva AS STRING),
            CAST(S.SKU AS STRING),
            S.Motivo,
            S.Sub_Motivo,
            S.Responsable,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        )

    """

    client.query(query).result()

#-------------------------------------------------------------------------------------------------
#--11. ELIMINAR TABLA TEMPORAL
#-------------------------------------------------------------------------------------------------

def eliminar_tabla_temporal(tabla_temporal):

    client.delete_table(
        tabla_temporal,
        not_found_ok=True
    )

#-------------------------------------------------------------------------------------------------
#--12. CREAR LA TABLA DETALLE
#-------------------------------------------------------------------------------------------------

def crear_tabla_detalle():

    query = f"""
    CREATE TABLE IF NOT EXISTS `{tabla("MOTIVO_DETALLE")}`
    (

        NUM_RESERVA STRING,
        SKU STRING,

        DESCRIPCION STRING,

        FECHA_INGRESADO DATETIME,
        FECHA_PREPARADO_TIENDA DATETIME,
        FECHA_PACTADA_RESERVA DATE,

        SERVICIO STRING,
        SUB_TIPO_SERVICIO STRING,

        CC_DESPACHA STRING,

        ON_TIME STRING,
        FLAGCUMPLE STRING,

        Control_Contabilizado STRING,
        Validacion_Conta STRING,

        Motivo STRING,
        Sub_Motivo STRING,
        Responsable STRING,

        HASH_RESERVA STRING,
        HIGHWATERMARK_PROPIO TIMESTAMP

    )

    CLUSTER BY NUM_RESERVA, SKU

    """

    client.query(query).result()

#-------------------------------------------------------------------------------------------------
#--13. ENRIQUECIMIENTO DE LA TABLA
#-------------------------------------------------------------------------------------------------

def query_detalle_incremental():

    return f"""

    SELECT *
    EXCEPT(rn)

    FROM (

        SELECT

            CAST(R.NUM_RESERVA AS STRING) AS NUM_RESERVA,

            UPPER(R.SKU) AS SKU,

            R.DESCRIPCION,

            R.FECHA_INGRESADO,
            R.FECHA_PREPARADO_TIENDA,
            R.FECHA_PACTADA_RESERVA,

            R.SERVICIO,
            R.SUB_TIPO_SERVICIO,

            CAST(R.CC_DESPACHA AS STRING)
                AS CC_DESPACHA,

            CAST(R.ON_TIME AS STRING)
                AS ON_TIME,

            CAST(R.FLAGCUMPLE AS STRING)
                AS FLAGCUMPLE,

            R.Control_Contabilizado,

            R.Validacion_Conta,

            M.Motivo,
            M.Sub_Motivo,
            M.Responsable,

            TO_HEX(
                MD5(
                    CONCAT(

                        CAST(R.NUM_RESERVA AS STRING),

                        UPPER(R.SKU),

                        COALESCE(
                            CAST(
                                R.FECHA_PACTADA_RESERVA
                                AS STRING
                            ),
                            ''
                        ),

                        COALESCE(
                            CAST(
                                R.FECHA_PREPARADO_TIENDA
                                AS STRING
                            ),
                            ''
                        ),

                        COALESCE(
                            R.SERVICIO,
                            ''
                        ),

                        COALESCE(
                            R.SUB_TIPO_SERVICIO,
                            ''
                        ),

                        COALESCE(
                            CAST(
                                R.CC_DESPACHA
                                AS STRING
                            ),
                            ''
                        ),

                        COALESCE(
                            CAST(R.ON_TIME AS STRING),
                            ''
                        ),

                        COALESCE(
                            CAST(R.FLAGCUMPLE AS STRING),
                            ''
                        ),

                        COALESCE(
                            R.Control_Contabilizado,
                            ''
                        ),

                        COALESCE(
                            R.Validacion_Conta,
                            ''
                        ),

                        COALESCE(
                            M.Motivo,
                            ''
                        ),

                        COALESCE(
                            M.Sub_Motivo,
                            ''
                        ),

                        COALESCE(
                            M.Responsable,
                            ''
                        )

                    )
                )
            ) AS HASH_RESERVA,

            CURRENT_TIMESTAMP()
                AS HIGHWATERMARK_PROPIO,

            ROW_NUMBER() OVER (

                PARTITION BY
                    CAST(R.NUM_RESERVA AS STRING),
                    UPPER(R.SKU)

                ORDER BY
                    R.FECHA_PREPARADO_TIENDA DESC,
                    R.FECHA_INGRESADO DESC

            ) AS rn

        FROM
        `sod-pe-bi-sandbox.Omnicanal_Pe.Reservas_PE` R

        LEFT JOIN (

            SELECT
                Reserva,
                SKU,
                Motivo,
                Sub_Motivo,
                Responsable

            FROM (

                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY Reserva, SKU
                        ORDER BY Fecha_Actualizacion DESC
                    ) AS rn

                FROM `{tabla("MOTIVO_MAESTRA")}`

            )

            WHERE rn = 1

        ) M

            ON CAST(R.NUM_RESERVA AS STRING)
                = M.Reserva

            AND

            UPPER(R.SKU)
                = UPPER(M.SKU)

        WHERE

            DATE(R.FECHA_PACTADA_RESERVA)
            >= DATE_SUB(
                CURRENT_DATE(),
                INTERVAL 4 DAY
            )

    )

    WHERE rn = 1

    """

#-------------------------------------------------------------------------------------------------
#--14. MERGE CON LA TABLA MOTIVO_DETALLE
#-------------------------------------------------------------------------------------------------

def merge_detalle():

    query = f"""

    MERGE `{tabla("MOTIVO_DETALLE")}` T

    USING (

        {query_detalle_incremental()}

    ) S

    ON

        T.NUM_RESERVA = S.NUM_RESERVA

        AND

        T.SKU = S.SKU

    WHEN MATCHED

        AND

        T.HASH_RESERVA <> S.HASH_RESERVA

    THEN

        UPDATE SET

            DESCRIPCION =
                S.DESCRIPCION,

            FECHA_INGRESADO =
                S.FECHA_INGRESADO,

            FECHA_PREPARADO_TIENDA =
                S.FECHA_PREPARADO_TIENDA,

            FECHA_PACTADA_RESERVA =
                S.FECHA_PACTADA_RESERVA,

            SERVICIO =
                S.SERVICIO,

            SUB_TIPO_SERVICIO =
                S.SUB_TIPO_SERVICIO,

            CC_DESPACHA =
                S.CC_DESPACHA,

            ON_TIME =
                S.ON_TIME,

            FLAGCUMPLE =
                S.FLAGCUMPLE,

            Control_Contabilizado =
                S.Control_Contabilizado,

            Validacion_Conta =
                S.Validacion_Conta,

            Motivo =
                S.Motivo,

            Sub_Motivo =
                S.Sub_Motivo,

            Responsable =
                S.Responsable,

            HASH_RESERVA =
                S.HASH_RESERVA,

            HIGHWATERMARK_PROPIO =
                CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN

        INSERT (

            NUM_RESERVA,
            SKU,

            DESCRIPCION,

            FECHA_INGRESADO,
            FECHA_PREPARADO_TIENDA,
            FECHA_PACTADA_RESERVA,

            SERVICIO,
            SUB_TIPO_SERVICIO,

            CC_DESPACHA,

            ON_TIME,
            FLAGCUMPLE,

            Control_Contabilizado,
            Validacion_Conta,

            Motivo,
            Sub_Motivo,
            Responsable,

            HASH_RESERVA,
            HIGHWATERMARK_PROPIO

        )

        VALUES (

            S.NUM_RESERVA,
            S.SKU,

            S.DESCRIPCION,

            S.FECHA_INGRESADO,
            S.FECHA_PREPARADO_TIENDA,
            S.FECHA_PACTADA_RESERVA,

            S.SERVICIO,
            S.SUB_TIPO_SERVICIO,

            S.CC_DESPACHA,

            S.ON_TIME,
            S.FLAGCUMPLE,

            S.Control_Contabilizado,
            S.Validacion_Conta,

            S.Motivo,
            S.Sub_Motivo,
            S.Responsable,

            S.HASH_RESERVA,
            S.HIGHWATERMARK_PROPIO

        )

    WHEN NOT MATCHED BY SOURCE

        AND

        T.FECHA_PACTADA_RESERVA >= DATE_SUB(
            CURRENT_DATE(),
            INTERVAL 4 DAY
        )

    THEN DELETE

    """

    client.query(query).result()

#-------------------------------------------------------------------------------------------------
#--15. ORQUESTADOR PRINCIPAL
#-------------------------------------------------------------------------------------------------


def procesar_archivo(df):

    df["SKU"] = df["SKU"].astype(str)
    df["Reserva"] = df["Reserva"].astype(str)
    
    df = df.drop_duplicates(
        subset=["Reserva", "SKU"],
        keep="last"
    )

    tabla_temporal = crear_tabla_temporal()

    try:

        print(f"Tabla Temporal: {tabla_temporal}")

        cargar_dataframe(
            df,
            tabla_temporal
        )

        print(" DataFrame cargado")

        crear_tabla_motivos()
        crear_tabla_detalle()

        print(" Tabla maestra validada")

        print("INICIO MERGE_MOTIVOS")
        merge_motivos(
            tabla_temporal
        )
        
        print("FIN MERGE_MOTIVOS")


        print(" Merge MOTIVO_MAESTRA ejecutado")
        print("INICIO MERGE_DETALLE")
        merge_detalle()
        print("FIN MERGE_DETALLE")

        print(" Merge MOTIVO_DETALLE ejecutado")
        print(" Merge ejecutado")

        return {
            "estado": "ok",
            "registros": len(df),
            "tabla_temporal": tabla_temporal
        }

    finally:

        eliminar_tabla_temporal(
            tabla_temporal
        )

        print(" Temporal eliminada")



