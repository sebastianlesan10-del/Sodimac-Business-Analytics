#-------------------------------------------------------------------------------------------------
#--1. LIBRERIAS
#-------------------------------------------------------------------------------------------------

from google.cloud import bigquery
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
import os

#-------------------------------------------------------------------------------------------------
#--2. CONFIGURACION
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
#--3. TABLAS
#-------------------------------------------------------------------------------------------------

def tabla(nombre_tabla: str) -> str:

    return (
        f"{BQ_PROJECT_ID}."
        f"{BQ_DATASET_ID}."
        f"{nombre_tabla}"
    )

#-------------------------------------------------------------------------------------------------
#--4. FILTROS DE AUDITORIA
#-------------------------------------------------------------------------------------------------

FILTROS_SERVICIO = {

    "DADc": [
        "DADc",
        "DADc VEV"
    ],

    "DADPis": [
        "DADt PIS",
        "DADt VE PIS",
        "RT PIS"
    ],

    "RT": [
        "RT STS",
        "RT STS VEV",
        "DADt STS",
        "DADt STS VEV"
    ]

}

#-------------------------------------------------------------------------------------------------
#--5. AUDITORIA PENDIENTES
#-------------------------------------------------------------------------------------------------

def obtener_pendientes(tipo_servicio):

    if tipo_servicio not in FILTROS_SERVICIO:

        raise ValueError(
            f"Tipo no válido: {tipo_servicio}"
        )

    filtro_sql = ",".join(

        f"'{valor}'"

        for valor

        in FILTROS_SERVICIO[tipo_servicio]

    )

    query = f"""

    SELECT

        NUM_RESERVA,
        SKU,
        Motivo,
        Sub_Motivo,
        Responsable,
        SUB_TIPO_SERVICIO,
        FECHA_PACTADA_RESERVA


    FROM `{tabla("MOTIVO_DETALLE")}`

    WHERE

        Control_Contabilizado = 'Contabilizado'

        AND ON_TIME = '0'

        AND (

            Motivo IS NULL

            OR

            TRIM(Motivo) = ''

        )

        AND

        SUB_TIPO_SERVICIO IN (

            {filtro_sql}

        )

    ORDER BY

        FECHA_PACTADA_RESERVA DESC

    """

    df = client.query(
        query
    ).to_dataframe()

    return df

#-------------------------------------------------------------------------------------------------
#--6. RESUMEN DE AUDITORIA
#-------------------------------------------------------------------------------------------------

def auditoria_resumen(tipo_servicio):

    df = obtener_pendientes(
        tipo_servicio
    )

    registros = df.to_dict(
        orient="records"
    )

    return {

        "tipo": tipo_servicio,

        "total_pendientes": len(df),

        "detalle": registros

    }
#-------------------------------------------------------------------------------------------------
#--7. GENERAR EXCEL
#-------------------------------------------------------------------------------------------------

def generar_excel_pendientes(
    tipo_servicio
):

    df = obtener_pendientes(
        tipo_servicio
    )

    
    ruta = (
        f"Pendientes_{tipo_servicio}_"
        f"{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    )


    df.to_excel(
        ruta,
        index=False
    )

    return ruta
#-------------------------------------------------------------------------------------------------
#--8. PRUEBA LOCAL
#-------------------------------------------------------------------------------------------------

if __name__ == "__main__":

    obtener_pendientes(
        "DADc"
    )