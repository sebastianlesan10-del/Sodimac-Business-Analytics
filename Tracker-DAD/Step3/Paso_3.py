# ------------------------------------------------------------------------------------------------------------------
# 1. LIBRERÍAS
# ------------------------------------------------------------------------------------------------------------------
import os
import sys
import time
import urllib3
import requests
import pandas as pd

from datetime import datetime
from datetime import timedelta

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

urllib3.disable_warnings()
from dotenv import load_dotenv


load_dotenv()
# ------------------------------------------------------------------------------------------------------------------
# 2. BIGQUERY
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

SERVICE_ACCOUNT_FILE = obtener_ruta_credenciales()

PROJECT_ID = os.getenv("BQ_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET_ID")


TABLE_MENSAJES = (
    f"{PROJECT_ID}.{DATASET_ID}.TRACKER_Envios_Mensajes"
)

TABLE_COORDENADAS = (
    f"{PROJECT_ID}.{DATASET_ID}.TRACKER_Coordenadas"
)



SCHEMA_COORDENADAS = [

    bigquery.SchemaField("NUM_RESERVA", "STRING"),

    bigquery.SchemaField("FONO_CLI", "STRING"),

    bigquery.SchemaField("NOMBRE", "STRING"),

    bigquery.SchemaField("LATITUD", "STRING"),

    bigquery.SchemaField("LONGITUD", "STRING"),

    bigquery.SchemaField("FECHA_COORDENADA", "STRING"),

    bigquery.SchemaField("FECHA_PROCESO", "DATETIME"),
    
    bigquery.SchemaField("ESTADO", "STRING"),
    
    bigquery.SchemaField("INTENTOS", "INTEGER")

]

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE
)

bq_cliente = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)


# ------------------------------------------------------------------------------------------------------------------
# 3. CONFIG API
# ------------------------------------------------------------------------------------------------------------------

URL_API = os.getenv("TENET_INFO_URL")
TOKEN_API = os.getenv("TENET_INFO_TOKEN")

HEADERS = {
    "apikey": TOKEN_API,
    "Content-Type": "application/json"
}


# ------------------------------------------------------------------------------------------------------------------
# 4. UTILIDADES
# ------------------------------------------------------------------------------------------------------------------

def tabla_existe(cliente, table_ref):
    try:
        cliente.get_table(table_ref)
        return True
    except NotFound:
        return False


def normalizar_telefono(tel):

    tel = (
        str(tel)
        .replace("+", "")
        .replace(" ", "")
        .strip()
    )

    if not tel.startswith("51"):
        tel = "51" + tel

    return tel




fecha_fin = datetime.now()
fecha_inicio = (
    fecha_fin - timedelta(days=1)
)


# ------------------------------------------------------------------------------------------------------------------
# 5. LEER STEP2 DESDE BIGQUERY
# ------------------------------------------------------------------------------------------------------------------

def leer_step2():

    query = f"""
    SELECT *
    FROM `{TABLE_MENSAJES}`
    """

    df = bq_cliente.query(query).to_dataframe()

    print(f" Registros Step2: {len(df)}")

    return df

# ------------------------------------------------------------------------------------------------------------------
# LEER STEP3 DESDE BIGQUERY
# ------------------------------------------------------------------------------------------------------------------

def leer_step3():

    if not tabla_existe(
        bq_cliente,
        TABLE_COORDENADAS
    ):
        return pd.DataFrame()

    query = f"""
    SELECT
        NUM_RESERVA,
        ESTADO,
        INTENTOS
    FROM `{TABLE_COORDENADAS}`
    """

    return bq_cliente.query(
        query
    ).to_dataframe()

# ------------------------------------------------------------------------------------------------------------------
# 6. EVITAR DUPLICADOS
# ------------------------------------------------------------------------------------------------------------------

def filtrar_no_procesados(df):

    if "ESTADO" not in df.columns:

        print(
            f" Pendientes Step3: {len(df)}"
        )

        return df

    df_filtrado = df[

        (
            df["ESTADO"].isna()
        )

        |

        (
            df["ESTADO"] == "SIN_RESPUESTA"
        )

    ]

    print(
        f" Pendientes Step3: "
        f"{len(df_filtrado)}"
    )

    return df_filtrado


# ------------------------------------------------------------------------------------------------------------------
# 7. CONSULTAR API
# ------------------------------------------------------------------------------------------------------------------

def consultar_api():


    body = {
        "celular": "51961113003",
        "fechaInicio": fecha_inicio.strftime("%d-%m-%Y"),
        "fechaFin": fecha_fin.strftime("%d-%m-%Y"),
        "opcion": 1
    }

    try:

        response = requests.post(
            URL_API,
            headers=HEADERS,
            json=body,
            verify=False
        )

        if response.status_code == 200:
            return response.json()

        return None

    except Exception as e:

        print(f" Error conexión API: {e}")

        return None




# ------------------------------------------------------------------------------------------------------------------
# 8 INDICE DE COORDENADAS (PARA SOPORTAR ESCALABILIDAD)
# ------------------------------------------------------------------------------------------------------------------
def crear_indice_coordenadas(data):

    indice = {}

    if not data:
        return indice

    mensajes = data.get("extraInfo", [])

    for m in mensajes:

        tel = normalizar_telefono(
            m.get("clientUid")
        )

        if not m.get("latitud") or not m.get("longitud"):
            continue

        if tel not in indice:
            indice[tel] = []

        indice[tel].append({
            "LATITUD": m["latitud"],
            "LONGITUD": m["longitud"],
            "FECHA_COORDENADA": m["fecha"]
        })

    return indice


# ------------------------------------------------------------------------------------------------------------------
# 9. PROCESAR
# ------------------------------------------------------------------------------------------------------------------

def procesar_coordenadas(df):

    resultados = []

    # ---------------------------------------------------------
    # UNA SOLA LLAMADA A LA API
    # ---------------------------------------------------------

    print(" Consultando API...")

    data = consultar_api()

    if not data:

        print(" No se obtuvo respuesta desde la API")

        return pd.DataFrame()

    print(" Respuesta API obtenida")

    # ---------------------------------------------------------
    # RECORRER RESERVAS
    # ---------------------------------------------------------

    indice = crear_indice_coordenadas(data)
    for _, row in df.iterrows():

        nuevo_intento = row["INTENTOS"] + 1

        telefono = normalizar_telefono(
            row.get("FONO_CLI", "")
        )

        reserva = str(row["NUM_RESERVA"])
        nombre = row.get("NOMBRE", "")
        print(
            f"\n Procesando reserva "
            f"{reserva} | Tel: {telefono}"
        )

        coords = indice.get(
                    telefono,
                    []
                )


        if coords:

            coords.sort(
                key=lambda x: x["FECHA_COORDENADA"],
                reverse=True
            )

            coords = coords[:1]

        print(
            f" Coordenadas encontradas: "
            f"{len(coords)}"
        )

        if not coords:

            
            estado = "SIN_RESPUESTA"

            resultados.append({

                "NUM_RESERVA": reserva,
                "FONO_CLI": telefono,
                "NOMBRE": nombre,

                "LATITUD": None,
                "LONGITUD": None,
                "FECHA_COORDENADA": None,

                "FECHA_PROCESO": datetime.now().replace(
                    microsecond=0
                ),

                "ESTADO": estado,
                "INTENTOS": nuevo_intento
            })

            print(
                f" {reserva} | {telefono} "
                f"{estado}"
            )

            continue


        for c in coords:

            resultados.append({

                "NUM_RESERVA": reserva,
                "FONO_CLI": telefono,
                "NOMBRE": nombre,
                "LATITUD": c["LATITUD"],
                "LONGITUD": c["LONGITUD"],
                "FECHA_COORDENADA": c["FECHA_COORDENADA"],
                "FECHA_PROCESO": datetime.now().replace(
                    microsecond=0
                ),
                "ESTADO": "ENCONTRADA",
                "INTENTOS": nuevo_intento
            })

            print(
                f" {reserva} | {telefono} -> "
                f"{c['LATITUD']}, {c['LONGITUD']}"
            )

    return pd.DataFrame(resultados)


# ------------------------------------------------------------------------------------------------------------------
# 10. GUARDAR BIGQUERY
# ------------------------------------------------------------------------------------------------------------------

def guardar_step3(df):

    if df.empty:

        print(" No hay coordenadas nuevas")

        return

    existe = tabla_existe(
        bq_cliente,
        TABLE_COORDENADAS
    )

    # ---------------------------------------------
    # CREAR TABLA SI NO EXISTE
    # ---------------------------------------------

    if not existe:

        job = bq_cliente.load_table_from_dataframe(
            df,
            TABLE_COORDENADAS,
            job_config=bigquery.LoadJobConfig(
                schema=SCHEMA_COORDENADAS,
                write_disposition="WRITE_TRUNCATE"
            )
        )

        job.result()

        print(
            f" Tabla creada con "
            f"{len(df)} registros"
        )

        return

    # ---------------------------------------------
    # TABLA STAGING
    # ---------------------------------------------

    staging_table = (
        f"{PROJECT_ID}.{DATASET_ID}."
        f"TRACKER_Coordenadas_STG"
    )

    job = bq_cliente.load_table_from_dataframe(
        df,
        staging_table,
        job_config=bigquery.LoadJobConfig(
            schema=SCHEMA_COORDENADAS,
            write_disposition="WRITE_TRUNCATE"
        )
    )

    job.result()

    # ---------------------------------------------
    # MERGE
    # ---------------------------------------------

    merge_sql = f"""
    MERGE `{TABLE_COORDENADAS}` T

    USING `{staging_table}` S

    ON T.NUM_RESERVA = S.NUM_RESERVA

    WHEN MATCHED THEN

    UPDATE SET

        T.FONO_CLI = S.FONO_CLI,
        T.NOMBRE = S.NOMBRE,
        T.LATITUD = S.LATITUD,
        T.LONGITUD = S.LONGITUD,
        T.FECHA_COORDENADA = S.FECHA_COORDENADA,
        T.FECHA_PROCESO = S.FECHA_PROCESO,
        T.ESTADO = S.ESTADO,
        T.INTENTOS = S.INTENTOS

    WHEN NOT MATCHED THEN

    INSERT (

        NUM_RESERVA,
        FONO_CLI,
        NOMBRE,
        LATITUD,
        LONGITUD,
        FECHA_COORDENADA,
        FECHA_PROCESO,
        ESTADO,
        INTENTOS

    )

    VALUES (

        S.NUM_RESERVA,
        S.FONO_CLI,
        S.NOMBRE,
        S.LATITUD,
        S.LONGITUD,
        S.FECHA_COORDENADA,
        S.FECHA_PROCESO,
        S.ESTADO,
        S.INTENTOS

    )
    """

    bq_cliente.query(
        merge_sql
    ).result()

    print(
        f" {len(df)} registros "
        f"actualizados mediante MERGE"
    )


# ------------------------------------------------------------------------------------------------------------------
# 11. FUNCIÓN PRINCIPAL
# ------------------------------------------------------------------------------------------------------------------

def ejecutar_paso3():

    df = leer_step2()

    df["FECHA_ENVIO"] = pd.to_datetime(
    df["FECHA_ENVIO"]
    )
    hoy = datetime.now().date()
    df = df[
        df["FECHA_ENVIO"].dt.date == hoy
    ]

    df = (
        df.sort_values("FECHA_ENVIO")
        .drop_duplicates(
            subset=["NUM_RESERVA"],
            keep="last"
        )
        .reset_index(drop=True)
    )

    df_hist = leer_step3()
    if df.empty:
        print(" No hay registros Step2")
        return

    if not df_hist.empty:
        df = df.merge(
            df_hist[
                [
                    "NUM_RESERVA",
                    "ESTADO",
                    "INTENTOS"
                ]
            ],

            on="NUM_RESERVA",
            how="left"
        )

    else:
        df["INTENTOS"] = 0


    df["INTENTOS"] = (
        df["INTENTOS"]
        .fillna(0)
        .astype(int)
    )


    df = filtrar_no_procesados(df)

    if df.empty:
        print(" Nada pendiente Step3")
        return

    df_resultado = procesar_coordenadas(df)
    guardar_step3(df_resultado)
    print("\n STEP3 COMPLETADO ")