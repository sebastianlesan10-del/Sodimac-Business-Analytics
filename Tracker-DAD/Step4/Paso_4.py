# ------------------------------------------------------------------------------------------------------------------
# STEP 4 - ALERTAS AUTOMÁTICAS DE COORDENADAS EN TEAMS
# ------------------------------------------------------------------------------------------------------------------

import os
import pandas as pd
from datetime import datetime
import smtplib
from dotenv import load_dotenv
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from email.mime.text import MIMEText

import sys

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound


load_dotenv()
# ------------------------------------------------------------------------------------------------------------------
# 1. RUTAS
# ------------------------------------------------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CARPETA_ALERTAS = os.path.join(
    BASE_DIR,
    "Alertas_Equipos"
)

os.makedirs(CARPETA_ALERTAS, exist_ok=True)


# ------------------------------------------------------------------------------------------------------------------
# BIGQUERY
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

TABLE_STEP1 = (
    f"{PROJECT_ID}.{DATASET_ID}.TRACKER_Reservas_Sin_Coord"
)

TABLE_STEP3 = (
    f"{PROJECT_ID}.{DATASET_ID}.TRACKER_Coordenadas"
)

TABLE_ALERTAS = (
    f"{PROJECT_ID}.{DATASET_ID}.TRACKER_Alertas_Teams"
)

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE
)

bq_cliente = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)


# ------------------------------------------------------------------------------------------------------------------
# 2. CONFIG TEAMS
# ------------------------------------------------------------------------------------------------------------------

DESTINATARIO_TEAMS = "Despachos DAD - Bodegas - Equipo Bodega <217e0f3b.falabella.onmicrosoft.com@amer.teams.ms>"

SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587

USUARIO = os.getenv("SMTP_USER")
PASSWORD = os.getenv("SMTP_PASSWORD")


# ------------------------------------------------------------------------------------------------------------------
# BIGQUERY UTILS
# ------------------------------------------------------------------------------------------------------------------

def tabla_existe(cliente, table_ref):
    try:
        cliente.get_table(table_ref)
        return True
    except NotFound:
        return False


SCHEMA_ALERTAS = [

    bigquery.SchemaField("NUM_RESERVA", "STRING"),

    bigquery.SchemaField("NOMBRE", "STRING"),

    bigquery.SchemaField("FONO_CLI", "STRING"),

    bigquery.SchemaField("REGION_DESP", "STRING"),
    bigquery.SchemaField("CIUDAD_DESP", "STRING"),
    bigquery.SchemaField("COMUNA_DESP", "STRING"),
    bigquery.SchemaField("DIRECCION_DESP", "STRING"),

    bigquery.SchemaField("LATITUD", "STRING"),
    bigquery.SchemaField("LONGITUD", "STRING"),

    bigquery.SchemaField("FECHA_COORDENADA", "STRING"),

    bigquery.SchemaField("FECHA_PROCESO", "DATETIME"),

    bigquery.SchemaField("FECHA_ALERTA", "DATETIME")
]

# ------------------------------------------------------------------------------------------------------------------
# 3. LEER STEP3
# ------------------------------------------------------------------------------------------------------------------

def leer_step3():

    query = f"""
    SELECT *
    FROM `{TABLE_STEP3}`
    WHERE ESTADO = 'ENCONTRADA'
      AND LATITUD IS NOT NULL
      AND LONGITUD IS NOT NULL
    """

    df = bq_cliente.query(query).to_dataframe()

    print(f" Registros Step3: {len(df)}")

    return df


# ------------------------------------------------------------------------------------------------------------------
# 3.1 AGREGAR DATOS DE STEP1
# ------------------------------------------------------------------------------------------------------------------

def enriquecer_con_step1(df):

    query = f"""
    SELECT DISTINCT

        NUM_RESERVA,

        REGION_DESP,
        CIUDAD_DESP,
        COMUNA_DESP,
        DIRECCION_DESP

    FROM `{TABLE_STEP1}`
    """

    df_step1 = bq_cliente.query(
        query
    ).to_dataframe()

    df["NUM_RESERVA"] = (
        df["NUM_RESERVA"]
        .astype(str)
        .str.strip()
    )

    df_step1["NUM_RESERVA"] = (
        df_step1["NUM_RESERVA"]
        .astype(str)
        .str.strip()
    )

    df = df.merge(
        df_step1,
        on="NUM_RESERVA",
        how="left"
    )

    print(" Datos Step1 agregados")

    return df


# ------------------------------------------------------------------------------------------------------------------
# 4. FILTRAR RESERVAS NO ALERTADAS
# ------------------------------------------------------------------------------------------------------------------

def filtrar_nuevas_coordenadas(df):

    if not tabla_existe(
        bq_cliente,
        TABLE_ALERTAS
    ):

        print(" Primera ejecución")

        return df

    query = f"""
    SELECT DISTINCT
        NUM_RESERVA
    FROM `{TABLE_ALERTAS}`
    """

    df_old = bq_cliente.query(
        query
    ).to_dataframe()

    reservas_alertadas = set(
        df_old["NUM_RESERVA"]
        .astype(str)
        .str.strip()
    )

    df_new = df[
        ~df["NUM_RESERVA"]
        .astype(str)
        .str.strip()
        .isin(reservas_alertadas)
    ]

    print(
        f" Nuevas coordenadas: {len(df_new)}"
    )

    return df_new


# ------------------------------------------------------------------------------------------------------------------
# 5. GENERAR EXCEL ALERTA
# ------------------------------------------------------------------------------------------------------------------

def generar_excel_alerta(df):

    fecha_archivo = datetime.now().strftime("%Y%m%d_%H%M%S")

    archivo_excel = os.path.join(
        CARPETA_ALERTAS,
        f"Coordenadas_Nuevas_{fecha_archivo}.xlsx"
    )

    columnas = [

        "NUM_RESERVA",
        "NOMBRE",
        "FONO_CLI",

        "REGION_DESP",
        "CIUDAD_DESP",
        "COMUNA_DESP",
        "DIRECCION_DESP",

        "LATITUD",
        "LONGITUD",

        "FECHA_COORDENADA",
        "FECHA_PROCESO"
    ]

    columnas_existentes = [
        c for c in columnas
        if c in df.columns
    ]

    df[columnas_existentes].to_excel(
        archivo_excel,
        index=False
    )

    print(f" Excel generado: {archivo_excel}")

    return archivo_excel


# ------------------------------------------------------------------------------------------------------------------
# 6. ENVIAR EXCEL A TEAMS
# ------------------------------------------------------------------------------------------------------------------

def enviar_excel_teams(ruta_excel, cantidad_registros):

    msg = MIMEMultipart()

    msg["Subject"] = (
        f" Coordenadas nuevas encontradas ({cantidad_registros})"
    )

    msg["From"] = USUARIO

    msg["To"] = DESTINATARIO_TEAMS

    cuerpo = f"""
        Hola equipo,

        Se encontraron {cantidad_registros} nuevas reservas con coordenadas.

        Adjunto encontrarán el archivo Excel para revisión.

        Proceso generado automáticamente.

        Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M')}
        """

    msg.attach(MIMEText(cuerpo, "plain"))

    with open(ruta_excel, "rb") as archivo:

        parte = MIMEBase(
            "application",
            "octet-stream"
        )

        parte.set_payload(
            archivo.read()
        )

    encoders.encode_base64(parte)

    parte.add_header(
        "Content-Disposition",
        f'attachment; filename="{os.path.basename(ruta_excel)}"'
    )

    msg.attach(parte)

    try:

        with smtplib.SMTP(
            SMTP_SERVER,
            SMTP_PORT
        ) as server:

            server.starttls()

            server.login(
                USUARIO,
                PASSWORD
            )

            server.send_message(msg)

        print(" Alerta enviada a Teams")

        return True

    except Exception as e:

        print(f" Error enviando alerta: {e}")

        return False


# ------------------------------------------------------------------------------------------------------------------
# 7. GUARDAR HISTÓRICO DE ALERTAS
# ------------------------------------------------------------------------------------------------------------------

def guardar_historico_alertas(df):

    if df.empty:
        return

    df_hist = df.copy()

    columnas_alerta = [

        "NUM_RESERVA",
        "NOMBRE",
        "FONO_CLI",

        "REGION_DESP",
        "CIUDAD_DESP",
        "COMUNA_DESP",
        "DIRECCION_DESP",

        "LATITUD",
        "LONGITUD",

        "FECHA_COORDENADA",
        "FECHA_PROCESO"
    ]

    df_hist = df_hist[columnas_alerta]

    df_hist["FECHA_ALERTA"] = (
        datetime.now()
        .replace(microsecond=0)
    )

    existe = tabla_existe(
        bq_cliente,
        TABLE_ALERTAS
    )

    if not existe:

        job = bq_cliente.load_table_from_dataframe(
            df_hist,
            TABLE_ALERTAS,
            job_config=bigquery.LoadJobConfig(
                schema=SCHEMA_ALERTAS,
                write_disposition="WRITE_TRUNCATE"
            )
        )

        job.result()

        print(
            f" Tabla creada con "
            f"{len(df_hist)} alertas"
        )

        return

    job = bq_cliente.load_table_from_dataframe(
        df_hist,
        TABLE_ALERTAS,
        job_config=bigquery.LoadJobConfig(
            schema=SCHEMA_ALERTAS,
            write_disposition="WRITE_APPEND"
        )
    )

    job.result()

    print(
        f" {len(df_hist)} alertas registradas"
    )


# ------------------------------------------------------------------------------------------------------------------
# 8. FUNCIÓN PRINCIPAL
# ------------------------------------------------------------------------------------------------------------------

def ejecutar_step4():

    df = leer_step3()
    
    df = df[
        (df["ESTADO"] == "ENCONTRADA")
        &
        (df["LATITUD"].notna())
        &
        (df["LONGITUD"].notna())
    ]


    if df.empty:

        return

    df = enriquecer_con_step1(df)

    df_new = filtrar_nuevas_coordenadas(df)

    if df_new.empty:

        print(" No existen coordenadas nuevas")

        return

    archivo_excel = generar_excel_alerta(df_new)

    enviado = enviar_excel_teams(
        archivo_excel,
        len(df_new)
    )

    if enviado:

        guardar_historico_alertas(df_new)

    print("\n STEP4 COMPLETADO ")


# ------------------------------------------------------------------------------------------------------------------
# 9. EJECUCIÓN
# ------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":

    ejecutar_step4()