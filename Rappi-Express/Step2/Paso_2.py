# ------------------------------------------------------------------------------------------------------------------
# 1. LIBRERÍAS
# ------------------------------------------------------------------------------------------------------------------
import os
import pandas as pd
import json
from datetime import datetime
import sys

from dotenv import load_dotenv

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound



load_dotenv()
# ------------------------------------------------------------------------------------------------------------------
# 2. RUTAS
# ------------------------------------------------------------------------------------------------------------------
def obtener_ruta_credenciales():

    if getattr(sys, "frozen", False):
        base_path = os.path.dirname(sys.executable)

    else:
        base_path = os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )

    return os.path.join(
        base_path,
        "sa-logistic-process-sod-pe-bi-sandbox.json"
    )

# ------------------------------------------------------------------------------------------------------------------
# VARIABLES CON CREDENCIALES Y ACCESOS
# ------------------------------------------------------------------------------------------------------------------

SERVICE_ACCOUNT_FILE = (
    obtener_ruta_credenciales()
)

PROJECT_ID = os.getenv(
    "BQ_PROJECT_ID"
)

DATASET_ID = os.getenv(
    "BQ_DATASET_ID"
)


# ------------------------------------------------------------------------------------------------------------------
# TABLAS
# ------------------------------------------------------------------------------------------------------------------

TABLE_STEP1 = (
    f"{PROJECT_ID}.{DATASET_ID}."
    f"RAPPI_Reservas_Sin_Coord"
)

TABLE_STEP2 = (
    f"{PROJECT_ID}.{DATASET_ID}."
    f"RAPPI_Reservas_Validador"
)


# ------------------------------------------------------------------------------------------------------------------
# DEFINICIÓN DEL ESQUEMA DE LA TABLA DEL STEP2
# ------------------------------------------------------------------------------------------------------------------

SCHEMA_STEP2 = [

    bigquery.SchemaField(
        "NUM_RESERVA",
        "STRING"
    ),

    bigquery.SchemaField(
        "FECHA_CREACION",
        "DATETIME"
    ),

    bigquery.SchemaField(
        "NOMBRE_CLI",
        "STRING"
    ),

    bigquery.SchemaField(
        "FONO_CLI",
        "STRING"
    ),

    bigquery.SchemaField(
        "DIRECCION_CLI",
        "STRING"
    ),

    bigquery.SchemaField(
        "REGION_CLI",
        "STRING"
    ),

    bigquery.SchemaField(
        "CIUDAD_CLI",
        "STRING"
    ),

    bigquery.SchemaField(
        "COMUNA_CLI",
        "STRING"
    ),

    bigquery.SchemaField(
        "CC_DESPACHA",
        "INTEGER"
    ),

    bigquery.SchemaField(
        "LATITUD",
        "STRING"
    ),

    bigquery.SchemaField(
        "LONGITUD",
        "STRING"
    ),

    bigquery.SchemaField(
        "TOTAL",
        "FLOAT"
    ),

    bigquery.SchemaField(
        "PRODUCTOS",
        "STRING"
    ),

    bigquery.SchemaField(
        "ENVIADO",
        "BOOL"
    ),

    
    bigquery.SchemaField(
        "FLAG_GEOCODIFICADO",
        "BOOL"
    )

]

# ------------------------------------------------------------------------------------------------------------------
# CLIENTE BIGQUERY
# ------------------------------------------------------------------------------------------------------------------

credentials = (
    service_account.Credentials
    .from_service_account_file(
        SERVICE_ACCOUNT_FILE
    )
)

bq_cliente = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)

# ------------------------------------------------------------------------------------------------------------------
# VALIDADOR DE EXISTENCIA DE TABLA
# ------------------------------------------------------------------------------------------------------------------
def tabla_existe(cliente, table_ref):

    try:

        cliente.get_table(table_ref)

        return True

    except NotFound:

        return False

# ------------------------------------------------------------------------------------------------------------------
# 3. LEER HISTÓRICO BASE
# ------------------------------------------------------------------------------------------------------------------
def leer_step1():

    query = f"""
    SELECT *
    FROM `{TABLE_STEP1}`
    """

    df = bq_cliente.query(
        query
    ).to_dataframe()

    print(
        f"Registros Step1: {len(df)}"
    )

    return df


# ------------------------------------------------------------------------------------------------------------------
# 4. TRANSFORMAR A NIVEL RESERVA (SIN PERDER DATA CLAVE)
# ------------------------------------------------------------------------------------------------------------------
def transformar_a_reserva(df):

    resultados = []

    for reserva, grupo in df.groupby("NUM_RESERVA"):

        try:
            row0 = grupo.iloc[0]

            fila = {
                # claves
                "NUM_RESERVA": reserva,
                "FECHA_CREACION": datetime.now(),

                # cliente
                "NOMBRE_CLI": row0["NOMBRE_CLI"],
                "FONO_CLI": row0["FONO_CLI"],
                "DIRECCION_CLI": row0["DIRECCION_CLI"],

                # ubicación (CRÍTICO para UBIGEO)
                "REGION_CLI": row0["REGION_CLI"],
                "CIUDAD_CLI": row0["CIUDAD_CLI"],
                "COMUNA_CLI": row0["COMUNA_CLI"],

                # logístico (CRÍTICO para IDCARGO)
                "CC_DESPACHA": row0["CC_DESPACHA"],

                # coordenadas (editarán operarios)
                "LATITUD": row0["LATITUD"],
                "LONGITUD": row0["LONGITUD"],

                # métricas
                "TOTAL": (grupo["CANTIDAD"] * grupo["PRECIO"]).sum(),

                # productos (detalle completo )
                "PRODUCTOS": json.dumps([
                    {
                        "sku": str(p["PRD_LVL_NUMBER"]).strip(),
                        "nombre": str(p["PRD_NAME_FULL"]).strip(),
                        "cantidad": int(p["CANTIDAD"]),
                        "precio": float(p["PRECIO"])
                    }
                    for _, p in grupo.iterrows()
                ]),

                # campo de control opcional 🔥
                "ENVIADO": False,
                "FLAG_GEOCODIFICADO": False
            }

            resultados.append(fila)

        except Exception as e:
            print(f" Error en reserva {reserva}: {e}")

    df_reservas = pd.DataFrame(resultados)

    print(f" Reservas generadas: {len(df_reservas)}")

    return df_reservas


# ------------------------------------------------------------------------------------------------------------------
# 5. FILTRAR SOLO NUEVAS RESERVAS
# ------------------------------------------------------------------------------------------------------------------
def guardar_step2(df):

    if df.empty:
        return

    existe = tabla_existe(
        bq_cliente,
        TABLE_STEP2
    )

    if not existe:

        job = (
            bq_cliente
            .load_table_from_dataframe(
                df,
                TABLE_STEP2,
                job_config=
                bigquery.LoadJobConfig(
                    schema=SCHEMA_STEP2,
                    write_disposition=
                    "WRITE_TRUNCATE"
                )
            )
        )

        job.result()

        print(
            f"Tabla creada con "
            f"{len(df)} registros"
        )

        return

    staging_table = (
        f"{PROJECT_ID}.{DATASET_ID}."
        f"RAPPI_Reservas_Validador_STG"
    )

    job = (
        bq_cliente
        .load_table_from_dataframe(
            df,
            staging_table,
            job_config=
            bigquery.LoadJobConfig(
                schema=SCHEMA_STEP2,
                write_disposition=
                "WRITE_TRUNCATE"
            )
        )
    )

    job.result()

    merge_sql = f"""

    MERGE `{TABLE_STEP2}` T

    USING `{staging_table}` S

    ON T.NUM_RESERVA = S.NUM_RESERVA

    WHEN NOT MATCHED THEN

    INSERT ROW

    """

    bq_cliente.query(
        merge_sql
    ).result()

    print(
        f"Step2 actualizado con "
        f"{len(df)} reservas"
    )




# ------------------------------------------------------------------------------------------------------------------
# 7. FUNCIÓN PRINCIPAL
# ------------------------------------------------------------------------------------------------------------------
def ejecutar_paso2():

    df = leer_step1()

    if df.empty:
        return

    df_reservas = transformar_a_reserva(df)

    guardar_step2(df_reservas)

    print("STEP2 COMPLETO")

