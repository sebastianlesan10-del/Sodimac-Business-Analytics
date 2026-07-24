# ------------------------------------------------------------------------------------------------------------------
# 1. LIBRERÍAS
# ------------------------------------------------------------------------------------------------------------------
import os
import sys
import time
import requests
import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor


from datetime import datetime

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
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


TABLE_RESERVAS = (
    f"{PROJECT_ID}.{DATASET_ID}.TRACKER_Reservas_Sin_Coord"
)

TABLE_ENVIOS = (
    f"{PROJECT_ID}.{DATASET_ID}.TRACKER_Envios_Mensajes"
)

TABLE_COORDENADAS = (
    f"{PROJECT_ID}.{DATASET_ID}.TRACKER_Coordenadas"
)

SCHEMA_ENVIOS = [

    bigquery.SchemaField("NUM_RESERVA", "STRING"),

    bigquery.SchemaField("FONO_CLI", "STRING"),

    bigquery.SchemaField("NOMBRE", "STRING"),

    bigquery.SchemaField("FECHA_ENVIO", "DATETIME"),

    bigquery.SchemaField("STATUS", "STRING"),

    bigquery.SchemaField("RESPUESTA", "STRING"),
]

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE
)

bq_cliente = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)


# ------------------------------------------------------------------------------------------------------------------
# 3. API
# ------------------------------------------------------------------------------------------------------------------

URL_API = os.getenv("TENET_SEND_URL")

HEADERS = {
    "Content-Type": "application/json"
}


# ------------------------------------------------------------------------------------------------------------------
# 4. VALIDAR TABLAS
# ------------------------------------------------------------------------------------------------------------------

def tabla_existe(cliente, table_ref):
    try:
        cliente.get_table(table_ref)
        return True
    except NotFound:
        return False


# ------------------------------------------------------------------------------------------------------------------
# NORMALIZAR NUMERO DE TELEFONO
# ------------------------------------------------------------------------------------------------------------------

def normalizar_telefono(telefono):

    # Nulos reales
    if pd.isna(telefono):
        return None

    tel = str(telefono).strip()

    # Nulos convertidos a texto
    if tel.lower() in {
        "",
        "null",
        "none",
        "nan",
        "n/a",
        "-"
    }:
        return None

    # conservar solo números
    tel = re.sub(r"[^0-9]", "", tel)

    if not tel:
        return None

    # 9XXXXXXXX -> 519XXXXXXXX
    if len(tel) == 9 and tel.startswith("9"):
        tel = f"51{tel}"

    # formato válido Perú móvil
    if not re.fullmatch(r"519\d{8}", tel):
        return None

    # blacklist
    if tel in {
        "51999999999",
        "51888888888",
        "51777777777"
    }:
        return None

    # repetitivos
    local = tel[2:]

    if len(set(local)) == 1:
        return None

    return tel

# ------------------------------------------------------------------------------------------------------------------
# FILTRAR TELEFONOS VALIDADOS
# ------------------------------------------------------------------------------------------------------------------

def filtrar_telefonos_validos(df):

    df = df.copy()

    df["FONO_NORMALIZADO"] = (
        df["FONO_CLI"]
        .apply(normalizar_telefono)
    )

    invalidos = df[
        df["FONO_NORMALIZADO"].isna()
    ]

    if not invalidos.empty:

        print(
            f" Telefónos descartados: "
            f"{len(invalidos)}"
        )

        print(
            invalidos[
                ["NUM_RESERVA", "FONO_CLI"]
            ].head(20)
        )

    df = df[
        df["FONO_NORMALIZADO"].notna()
    ]

    df["FONO_CLI"] = df[
        "FONO_NORMALIZADO"
    ]

    df.drop(
        columns=["FONO_NORMALIZADO"],
        inplace=True
    )

    print(
        f" Teléfonos válidos: {len(df)}"
    )

    print(f"Descartados: {len(invalidos)}")

    return df

# ------------------------------------------------------------------------------------------------------------------
# 5. LEER RESERVAS DESDE BIGQUERY
# ------------------------------------------------------------------------------------------------------------------

def leer_reservas():

    query = f"""
    SELECT *
    FROM `{TABLE_RESERVAS}`
    """

    df = bq_cliente.query(query).to_dataframe()

    print(f" Reservas encontradas: {len(df)}")

    return df


# ------------------------------------------------------------------------------------------------------------------
# LEER REENVIOS DESDE EL STEP 3
# ------------------------------------------------------------------------------------------------------------------

def leer_reenvios():

    if not tabla_existe(
        bq_cliente,
        TABLE_COORDENADAS
    ):
        return pd.DataFrame()

    query = f"""
    SELECT
        c.NUM_RESERVA,
        c.FONO_CLI,
        c.NOMBRE AS NOMBRE_CLI,
        c.ESTADO,
        c.INTENTOS
    FROM `{TABLE_COORDENADAS}` c
    INNER JOIN `{TABLE_ENVIOS}` e
        ON c.NUM_RESERVA = e.NUM_RESERVA
    WHERE c.ESTADO = 'SIN_RESPUESTA'
      AND DATE(e.FECHA_ENVIO) = CURRENT_DATE()
    """

    df = bq_cliente.query(query).to_dataframe()

    print(
        f" Reenvíos candidatos: {len(df)}"
    )

    return df

# ------------------------------------------------------------------------------------------------------------------
# 6. UNA RESERVA = UN MENSAJE
# ------------------------------------------------------------------------------------------------------------------

def preparar_reservas(df):

    df = (
        df.sort_values("FECHA_PROCESO")
          .drop_duplicates(subset=["NUM_RESERVA"])
    )

    print(f" Reservas únicas: {len(df)}")

    return df


# ------------------------------------------------------------------------------------------------------------------
# 7. FILTRAR YA ENVIADOS
# ------------------------------------------------------------------------------------------------------------------

def filtrar_no_enviados(df):

    if not tabla_existe(bq_cliente, TABLE_ENVIOS):

        print(" No existe histórico de envíos")

        return df

    query = f"""
    SELECT DISTINCT
        NUM_RESERVA
    FROM `{TABLE_ENVIOS}`
    """

    df_env = bq_cliente.query(query).to_dataframe()

    enviados = set(
        df_env["NUM_RESERVA"]
        .astype(str)
        .str.strip()
    )

    df_filtrado = df[
        ~df["NUM_RESERVA"]
        .astype(str)
        .str.strip()
        .isin(enviados)
    ]

    print(f" Pendientes de envío: {len(df_filtrado)}")

    return df_filtrado


# ------------------------------------------------------------------------------------------------------------------
# 8. ENVÍO API
# ------------------------------------------------------------------------------------------------------------------

def enviar_mensaje(row):

    telefono = (
        str(row.get("FONO_CLI", ""))
        .replace(" ", "")
        .strip()
    )

    
    nombre_raw = (
        row.get("NOMBRE_CLI")
        or row.get("NOMBRE")
        or "CLIENTE"
    )
    nombre = str(nombre_raw).split()[0]


    reserva = str(row["NUM_RESERVA"])

    params = f"{nombre}-{reserva}"

    data = {
        "accountId": "sodimac-lg",
        "fromUid": "51961113003",
        "clientUid": telefono,
        "template": "76ec26a4-111a-4f3a-9cf8-f9fee812d29a",
        "params": params
    }

    try:

        inicio = time.time()

        response = requests.post(
            URL_API,
            headers=HEADERS,
            json=data,
            timeout=30
        )

        fin = time.time()

        print(
            f" Reserva {reserva} -> "
            f"{response.status_code} "
            f"({fin - inicio:.2f}s)"
        )

        return {
            "NUM_RESERVA": reserva,
            "FONO_CLI": telefono,
            "NOMBRE": nombre,
            "FECHA_ENVIO": datetime.now().replace(microsecond=0),
            "STATUS": str(response.status_code),
            "RESPUESTA": response.text
        }

    except Exception as e:

        print(f" Error en reserva {reserva}: {e}")

        return {
            "NUM_RESERVA": reserva,
            "FONO_CLI": telefono,
            "NOMBRE": nombre,
            "FECHA_ENVIO": datetime.now().replace(microsecond=0),
            "STATUS": "ERROR",
            "RESPUESTA": str(e)
        }


# ------------------------------------------------------------------------------------------------------------------
# 9. GUARDAR RESULTADOS BIGQUERY
# ------------------------------------------------------------------------------------------------------------------

def guardar_envios(resultados):

    if not resultados:
        return

    df_new = pd.DataFrame(resultados)

    existe = tabla_existe(
        bq_cliente,
        TABLE_ENVIOS
    )

    if not existe:

        job = bq_cliente.load_table_from_dataframe(
            df_new,
            TABLE_ENVIOS,
            job_config=bigquery.LoadJobConfig(
                schema=SCHEMA_ENVIOS,
                write_disposition="WRITE_TRUNCATE"
            )
        )

        job.result()

        print(
            f" Tabla creada con "
            f"{len(df_new)} envíos"
        )

        return

    job = bq_cliente.load_table_from_dataframe(
        df_new,
        TABLE_ENVIOS,
        job_config=bigquery.LoadJobConfig(
            schema=SCHEMA_ENVIOS,
            write_disposition="WRITE_APPEND"
        )
    )

    job.result()

    print(
        f" {len(df_new)} envíos registrados"
    )


# ------------------------------------------------------------------------------------------------------------------
# 10. FUNCIÓN PRINCIPAL
# ------------------------------------------------------------------------------------------------------------------

def ejecutar_paso2():

    # ---------------------------------------------------------
    # MENSAJES NUEVOS
    # ---------------------------------------------------------

    df_nuevos = leer_reservas()

    if not df_nuevos.empty:

        df_nuevos = preparar_reservas(
            df_nuevos
        )

        df_nuevos = filtrar_no_enviados(
            df_nuevos
        )

    # ---------------------------------------------------------
    # REENVÍOS
    # ---------------------------------------------------------

    df_reenvios = leer_reenvios()

    # ---------------------------------------------------------
    # UNIR AMBAS FUENTES
    # ---------------------------------------------------------

    df = pd.concat(
        [
            df_nuevos,
            df_reenvios
        ],
        ignore_index=True
    )

    df = filtrar_telefonos_validos(df)

    print(
        f" Nuevos: {len(df_nuevos)}"
    )

    print(
        f" Reenvíos: {len(df_reenvios)}"
    )

    print(
        f" Total a enviar: {len(df)}"
    )

    if df.empty:

        print(
            " No hay envíos pendientes"
        )

        return

    # ---------------------------------------------------------
    # ENVÍO
    # ---------------------------------------------------------

    print(
        f" Enviando {len(df)} "
        f"mensajes en paralelo..."
    )

    rows = [
        row
        for _, row in df.iterrows()
    ]

    inicio = time.time()

    with ThreadPoolExecutor(
        max_workers=5
    ) as executor:

        resultados = list(
            executor.map(
                enviar_mensaje,
                rows
            )
        )

    fin = time.time()

    print(
        f" Envíos completados en "
        f"{fin - inicio:.2f} segundos"
    )

    guardar_envios(
        resultados
    )

    print(
        " Proceso completado"
    )