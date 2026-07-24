# ------------------------------------------------------------------------------------------------------------------
# STEP 4 - PROCESAR RESPUESTAS Y GENERAR TRACKING
# ------------------------------------------------------------------------------------------------------------------

import os
import pandas as pd
import json
from datetime import datetime
import requests
import smtplib
from email.mime.text import MIMEText

import sys

from dotenv import load_dotenv

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound


load_dotenv()


# ------------------------------------------------------------------------------------------------------------------
# CONFIGURACIÓN
# ------------------------------------------------------------------------------------------------------------------

def obtener_ruta_credenciales():

    if getattr(sys, "frozen", False):

        base_path = os.path.dirname(
            sys.executable
        )

    else:

        base_path = os.path.dirname(
            os.path.dirname(
                os.path.abspath(__file__)
            )
        )

    return os.path.join(
        base_path,
        "sa-logistic-process-sod-pe-bi-sandbox.json"
    )

# ------------------------------------------------------------------------------------------------------------------
# VARIABLES DE ENTORNO
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

SMTP_USER = os.getenv(
    "SMTP_USER"
)

SMTP_PASSWORD = os.getenv(
    "SMTP_PASSWORD"
)

# ------------------------------------------------------------------------------------------------------------------
# TABLAS BIGQUERY
# ------------------------------------------------------------------------------------------------------------------

TABLE_ENVIOS = (
    f"{PROJECT_ID}.{DATASET_ID}."
    f"RAPPI_Envios"
)

TABLE_TRACKING = (
    f"{PROJECT_ID}.{DATASET_ID}."
    f"RAPPI_Tracking"
)


# ------------------------------------------------------------------------------------------------------------------
# ESQUEMA DE TRACKING
# ------------------------------------------------------------------------------------------------------------------

SCHEMA_TRACKING = [

    bigquery.SchemaField(
        "NUM_RESERVA",
        "STRING"
    ),

    bigquery.SchemaField(
        "FECHA_ENVIO",
        "DATETIME"
    ),

    bigquery.SchemaField(
        "STATUS_API",
        "STRING"
    ),

    bigquery.SchemaField(
        "ESTADO",
        "STRING"
    ),

    bigquery.SchemaField(
        "TRACKING_URL",
        "STRING"
    ),

    bigquery.SchemaField(
        "TOTAL",
        "FLOAT"
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
# EXISTENCIA DE TABLA
# ------------------------------------------------------------------------------------------------------------------

def tabla_existe(cliente, table_ref):

    try:

        cliente.get_table(
            table_ref
        )

        return True

    except NotFound:

        return False
    


# ------------------------------------------------------------------------------------------------------------------
# 1. LEER HISTÓRICO DE ENVÍOS
# ------------------------------------------------------------------------------------------------------------------
def leer_envios():

    query = f"""
    SELECT *
    FROM `{TABLE_ENVIOS}`
    """

    df = (
        bq_cliente
        .query(query)
        .to_dataframe()
    )

    print(
        f"Registros de envíos: {len(df)}"
    )

    return df


# ------------------------------------------------------------------------------------------------------------------
# 2. PROCESAR RESPUESTA
# ------------------------------------------------------------------------------------------------------------------
def procesar_respuestas(df):

    resultados = []

    for _, row in df.iterrows():

        try:

            payload = json.loads(row["PAYLOAD"])

            # respuesta puede venir vacía o mal formada
            respuesta = {}
            if "RESPONSE" in row and pd.notna(row["RESPONSE"]):
                try:
                    respuesta = json.loads(row["RESPONSE"])
                except:
                    respuesta = {}

            status = str(row["STATUS"])

            # intentar obtener tracking
            tracking_url = None

            if isinstance(respuesta, dict):
                tracking_url = respuesta.get("tracking_url") or respuesta.get("trackingUrl")

            # clasificar estado
            estado = "ERROR"

            if status in ["200", "201"]:
                estado = "ENVIADO"

            if not tracking_url and estado == "ENVIADO":
                estado = "SIN_TRACKING"
            

            resultados.append({
                "NUM_RESERVA": row["NUM_RESERVA"],
                "FECHA_ENVIO": row["FECHA_ENVIO"],
                "STATUS_API": status,
                "ESTADO": estado,
                "TRACKING_URL": tracking_url,
                "TOTAL": payload.get("total_value"),
            })

        except Exception as e:
            print(f" Error procesando fila: {e}")

    df_result = pd.DataFrame(resultados)

    print(f" Registros procesados: {len(df_result)}")

    return df_result

# ------------------------------------------------------------------------------------------------------------------
# 3. ALERTAS AUTOMATICAS EN TEAMS
# ------------------------------------------------------------------------------------------------------------------
def enviar_alerta_teams_email(mensaje):

    destinatario = "xxxxxx.falabella.onmicrosoft.com@amer.teams.ms"

    msg = MIMEText(mensaje)
    msg['Subject'] = 'Alerta Rappi'
    msg['From'] = SMTP_USER
    msg['To'] = destinatario

    try:
        with smtplib.SMTP('smtp.office365.com', 587) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)

        print(" Alerta consolidada enviada a Teams")

    except Exception as e:
        print(f"Error enviando correo a Teams: {e}")
# ------------------------------------------------------------------------------------------------------------------
# 4. EVITAR DUPLICADOS
# ------------------------------------------------------------------------------------------------------------------
def filtrar_nuevos(df):

    if not tabla_existe(
        bq_cliente,
        TABLE_TRACKING
    ):
        return df

    query = f"""
    SELECT DISTINCT
        NUM_RESERVA
    FROM `{TABLE_TRACKING}`
    """

    df_old = (
        bq_cliente
        .query(query)
        .to_dataframe()
    )

    enviados = set(
        df_old["NUM_RESERVA"]
        .astype(str)
    )

    df = df[
        ~df["NUM_RESERVA"]
        .astype(str)
        .isin(enviados)
    ]

    print(
        f" Nuevos tracking: {len(df)}"
    )

    return df


# ------------------------------------------------------------------------------------------------------------------
# 5. GUARDAR TRACKING
# ------------------------------------------------------------------------------------------------------------------
def guardar_tracking(df):

    if df.empty:

        print(
            " Nada que guardar"
        )

        return

    existe = tabla_existe(
        bq_cliente,
        TABLE_TRACKING
    )

    if not existe:

        job = (
            bq_cliente
            .load_table_from_dataframe(

                df,

                TABLE_TRACKING,

                job_config=
                bigquery.LoadJobConfig(

                    schema=SCHEMA_TRACKING,

                    write_disposition=
                    "WRITE_TRUNCATE"

                )
            )
        )

        job.result()

        print(
            f" Tracking creado: "
            f"{len(df)} registros"
        )

        return

    job = (
        bq_cliente
        .load_table_from_dataframe(

            df,

            TABLE_TRACKING,

            job_config=
            bigquery.LoadJobConfig(

                schema=SCHEMA_TRACKING,

                write_disposition=
                "WRITE_APPEND"

            )
        )
    )

    job.result()

    print(
        f" {len(df)} tracking agregados"
    )

# ------------------------------------------------------------------------------------------------------------------
# 6. FUNCIÓN PRINCIPAL
# ------------------------------------------------------------------------------------------------------------------
def ejecutar_step4():

    df = leer_envios()

    if df.empty:
        return

    df_proc = procesar_respuestas(df)

    df_new = filtrar_nuevos(df_proc)

    #  LISTA PARA ACUMULAR MENSAJES
    alertas = []
    
    for _, row in df_new.iterrows():
        if row["ESTADO"] == "ENVIADO" and pd.notna(row["TRACKING_URL"]):
            linea = f"""Reserva: {row['NUM_RESERVA']}
            Tracking: {row['TRACKING_URL']}
            """
            alertas.append(linea)

    
    if alertas:
        mensaje_final = "NUEVAS ÓRDENES RAPPI\n\n" + "\n".join(alertas)
        enviar_alerta_teams_email(mensaje_final)

    guardar_tracking(df_new)

    print("\n STEP4 COMPLETADO ")


