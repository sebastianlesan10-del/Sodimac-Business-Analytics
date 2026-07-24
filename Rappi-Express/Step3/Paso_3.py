# ------------------------------------------------------------------------------------------------------------------
# 1. LIBRERÍAS
# ------------------------------------------------------------------------------------------------------------------
import os
import pandas as pd
import numpy as np
import json
import requests
from datetime import datetime
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import sys
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound



load_dotenv()
# ------------------------------------------------------------------------------------------------------------------
# 2. CONFIGURACIÓN
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


# ------------------------------------------------------------------------------------------------------------------
# TABLAS DE BIGQUERY
# ------------------------------------------------------------------------------------------------------------------

TABLE_STEP2 = (
    f"{PROJECT_ID}.{DATASET_ID}."
    f"RAPPI_Reservas_Validador"
)

TABLE_STEP2_STG = (
    f"{PROJECT_ID}.{DATASET_ID}."
    f"RAPPI_Reservas_Validador_STG"
)

TABLE_ENVIOS = (
    f"{PROJECT_ID}.{DATASET_ID}."
    f"RAPPI_Envios"
)


# ------------------------------------------------------------------------------------------------------------------
# ESQUEMA DE ENVIOS
# ------------------------------------------------------------------------------------------------------------------

SCHEMA_ENVIOS = [

    bigquery.SchemaField("NUM_RESERVA", "STRING"),

    bigquery.SchemaField(
        "FECHA_ENVIO",
        "DATETIME"
    ),

    bigquery.SchemaField(
        "STATUS",
        "STRING"
    ),

    bigquery.SchemaField(
        "PAYLOAD",
        "STRING"
    ),

    bigquery.SchemaField(
        "RESPONSE",
        "STRING"
    )
]

# ------------------------------------------------------------------------------------------------------------------
# CLIENTES DE BIGQUERY
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
# EXISTENCIA DE LA TABLA
# ------------------------------------------------------------------------------------------------------------------

def tabla_existe(cliente, table_ref):

    try:

        cliente.get_table(table_ref)

        return True

    except NotFound:

        return False


MODO_PRUEBA = False  # NO ENVÍA A RAPPI


# tablas externas
UBIGEO_FILE = r"C:\Users\jsantoses\OneDrive - Falabella\Escritorio\OneDrive - Falabella\Logistica Omnicanal - 01. Planificación Omnicanal\20. Digitalizacion\Coordenadas_Rappi\Tabla_Ubigeos_Actualizados_Base.xlsx"
CC_FILE = r"C:\Users\jsantoses\OneDrive - Falabella\Escritorio\OneDrive - Falabella\Logistica Omnicanal - 01. Planificación Omnicanal\20. Digitalizacion\Coordenadas_Rappi\CARGO_CC_TIENDA.xlsx"



# API RAPPI
API_URL = "https://services.xxxxxxxxxxx/xxxxxxxxxxxxxxxx/xxxxxxxx"
GOOGLE_API_KEY = "AxxxxxxxxxIzaSysaTkCxxxxxxxxxxxVOxxxxxxxU"

HEADERS = {
    "Content-Type": "application/json",
    "retailer_id": "xxxxxxxxxx",
    "user_id": "xxxx",
    "user-token": "xxxxxxxxxxxx",
    "Retailer": "Sodimac"
}




# ------------------------------------------------------------------------------------------------------------------
# 2.5. LIMPIEZA
# ------------------------------------------------------------------------------------------------------------------

def limpiar_y_validar_coordenadas(lat, lng):

    try:
        #  SI ES NÚMERO → ya está bien
        if isinstance(lat, (int, float, np.number)) and isinstance(lng, (int, float, np.number)):
            lat = float(lat)
            lng = float(lng)
            
            lat = float(f"{lat:.7f}")
            lng = float(f"{lng:.7f}")

        else:
            #  si es texto
            lat = str(lat).strip().replace(",", ".")
            lng = str(lng).strip().replace(",", ".")

            lat = float(lat)
            lng = float(lng)


            
            lat = float(f"{lat:.7f}")
            lng = float(f"{lng:.7f}")


        #  SOLO bloquear 0 (como pediste)
        if lat == 0 or lng == 0:
            return None, None

        return lat, lng

    except Exception as e:
        print(f" ERROR COORD → {lat}, {lng} → {e}")
        return None, None

# ------------------------------------------------------------------------------------------------------------------
# 3 PRECISIÓN
# ------------------------------------------------------------------------------------------------------------------

def asegurar_precision(x):
    try:
        x = float(x)

        # si tiene menos de 3 decimales reales → ajustar
        if round(x, 2) == x:
            x = x + 0.000001

        return round(x, 6)

    except:
        return None

# ------------------------------------------------------------------------------------------------------------------
# 3.1 LIMPIAR DIRECCIÓNES PARA LA API MAPS
# ------------------------------------------------------------------------------------------------------------------
def limpiar_direccion(row):
    try:
        direccion = str(row.get("DIRECCION_CLI", "")).strip()
        comuna_raw = str(row.get("COMUNA_CLI", "")).strip()       
        if " >" in comuna_raw:
            comuna = comuna_raw.split(" >")[0].strip()
        else:
            comuna = comuna_raw


        ciudad = str(row.get("CIUDAD_CLI", "")).strip()
        direccion_final = f"{direccion}, {comuna}, {ciudad}, PERU"

        return direccion_final

    except:
        return None
# ------------------------------------------------------------------------------------------------------------------
# 3.2 FUNCIÓN PARA OBTENER LAS COORDENAS CON API DE GOOGLE MAPS
# ------------------------------------------------------------------------------------------------------------------

def obtener_coordenadas_google(direccion):

    url = "https://maps.googleapis.com/maps/api/geocode/json"

    params = {
        "address": direccion,
        "key": GOOGLE_API_KEY
    }

    try:
        response = requests.get(url, params=params, verify=False)
        data = response.json()

        if data.get("status") == "OK":
            loc = data["results"][0]["geometry"]["location"]
            return loc["lat"], loc["lng"]
        else:
            print(f" Google error: {data.get('status')}")
            return None, None

    except Exception as e:
        print(f" Error Google: {e}")
        return None, None

# ------------------------------------------------------------------------------------------------------------------
# 3.5 LEER BASE (STEP2)
# ------------------------------------------------------------------------------------------------------------------
def leer_base():

    query = f"""
    SELECT *
    FROM `{TABLE_STEP2}`
    """

    df = bq_cliente.query(
        query
    ).to_dataframe()

    print(
        f"Reservas en Step2: {len(df)}"
    )

    return df


# ------------------------------------------------------------------------------------------------------------------
# ACTUALIZAR TABLA STEP2
# ------------------------------------------------------------------------------------------------------------------
def actualizar_step2(df):

    columnas_actualizar = [

        "NUM_RESERVA",
        "LATITUD",
        "LONGITUD",
        "FLAG_GEOCODIFICADO"

    ]

    df_update = (
        df[columnas_actualizar]
        .copy()
    )

    # Mantener mismo tipo que Step2 (STRING)
    df_update["LATITUD"] = (
        df_update["LATITUD"]
        .astype(str)
    )

    df_update["LONGITUD"] = (
        df_update["LONGITUD"]
        .astype(str)
    )

    # NUM_RESERVA como STRING para evitar problemas en el MERGE
    df_update["NUM_RESERVA"] = (
        df_update["NUM_RESERVA"]
        .astype(str)
    )

    job = (
        bq_cliente
        .load_table_from_dataframe(

            df_update,

            TABLE_STEP2_STG,

            job_config=
            bigquery.LoadJobConfig(

                write_disposition=
                "WRITE_TRUNCATE"

            )
        )
    )

    job.result()

    merge_sql = f"""

    MERGE `{TABLE_STEP2}` T

    USING `{TABLE_STEP2_STG}` S

    ON T.NUM_RESERVA = S.NUM_RESERVA

    WHEN MATCHED THEN

    UPDATE SET

        T.LATITUD = S.LATITUD,
        T.LONGITUD = S.LONGITUD,
        T.FLAG_GEOCODIFICADO = S.FLAG_GEOCODIFICADO

    """

    bq_cliente.query(
        merge_sql
    ).result()

    print(
        "Step2 actualizado en BigQuery"
    )
# ------------------------------------------------------------------------------------------------------------------
# 4. FILTRAR LISTOS PARA ENVÍO
# ------------------------------------------------------------------------------------------------------------------
def filtrar_listos(df):

    # solo filtrar que existan (no vacíos)
    df = df[
        df["LATITUD"].notna()
        &
        df["LONGITUD"].notna()
    ]

    print(
        f" Con coordenadas (sin validar aún): {len(df)}"
    )

    # evitar reenvíos exitosos
    if tabla_existe(
        bq_cliente,
        TABLE_ENVIOS
    ):

        query = f"""
        SELECT DISTINCT
            NUM_RESERVA
        FROM `{TABLE_ENVIOS}`
        WHERE CAST(STATUS AS STRING)
        IN ('200', '201')
        """

        df_old = (
            bq_cliente
            .query(query)
            .to_dataframe()
        )

        enviados_exitosos = set(
            df_old["NUM_RESERVA"]
            .astype(str)
        )

        df = df[
            ~df["NUM_RESERVA"]
            .astype(str)
            .isin(enviados_exitosos)
        ]

        print(
            f" Pendientes: {len(df)}"
        )

    return df


# ------------------------------------------------------------------------------------------------------------------
# 5. ENRIQUECER (UBIGEO + IDCARGO)
# ------------------------------------------------------------------------------------------------------------------
def enriquecer(df):

    dfU = pd.read_excel(UBIGEO_FILE)
    dfCC = pd.read_excel(CC_FILE)

    # limpiar texto
    for c in ["REGION_CLI", "CIUDAD_CLI", "COMUNA_CLI"]:
        df[c] = df[c].astype(str).str.strip()

    for c in ["Bd_Comunas_Total[Departamento]", "Bd_Comunas_Total[Ciudad]", "Comuna_Sodimac"]:
        dfU[c] = dfU[c].astype(str).str.strip()

    # UBIGEO
    df = df.merge(
        dfU,
        how="left",
        left_on=["REGION_CLI", "CIUDAD_CLI", "COMUNA_CLI"],
        right_on=[
            "Bd_Comunas_Total[Departamento]",
            "Bd_Comunas_Total[Ciudad]",
            "Comuna_Sodimac"
        ]
    )

    # IDCARGO
    df["CC_DESPACHA"] = pd.to_numeric(df["CC_DESPACHA"], errors="coerce")
    dfCC["CC"] = pd.to_numeric(dfCC["CC"], errors="coerce")

    df = df.merge(
        dfCC[["CC", "IDCARGO"]],
        how="left",
        left_on="CC_DESPACHA",
        right_on="CC"
    )

    return df


# ------------------------------------------------------------------------------------------------------------------
# 6. CONSTRUIR PAYLOAD
# ------------------------------------------------------------------------------------------------------------------
def convert_to_native(value):
    if pd.isna(value):
        return None
    return value

def separar_nombre_apellido(nombre_completo):
    if pd.isna(nombre_completo) or nombre_completo == "":
        return "", ""
    
    palabras = str(nombre_completo).strip().split()
    
    if len(palabras) == 0:
        return "", ""
    elif len(palabras) == 1:
        return palabras[0], palabras[0]
    elif len(palabras) == 2:
        return palabras[0], palabras[1]
    elif len(palabras) == 3:
        return palabras[0], f"{palabras[1]} {palabras[2]}"
    elif len(palabras) == 4:
        return f"{palabras[0]} {palabras[1]}", f"{palabras[2]} {palabras[3]}"
    else:
        nombre = f"{palabras[0]} {palabras[1]}"
        apellido = " ".join(palabras[2:])
        return nombre, apellido
    


def construir_payload(row, lat, lng):

    productos_raw = json.loads(row["PRODUCTOS"])

    # limpiar productos correctamente
    
    productos = [
        {
            "product_id": str(p.get("sku")).strip().replace(" ", ""),
            "name": str(p.get("nombre")).strip(),
            "description": str(p.get("nombre")).strip(),
            "units": int(p.get("cantidad", 0)),
            "ean": str(p.get("sku")).strip().replace(" ", ""),
            "unit_price": int(p.get("precio", 0))
        }
        for p in productos_raw
        if not pd.isna(p.get("sku")) and str(p.get("sku")).strip() != ""
    ]

    nombre_completo = convert_to_native(row.get("NOMBRE_CLI", ""))
    first_name, last_name = separar_nombre_apellido(nombre_completo)


    body = {
        "total_value": int(row["TOTAL"]),
        "user_tip": 0,
        "vehicle_type": "BIKE",
        "payment_method": "ONLINE",
        "order_id": int(row["NUM_RESERVA"]),
        "in_store_reference_id": convert_to_native(row.get("IDCARGO")),

        "action_points": [
            {
                "picking_point_id": convert_to_native(row.get("IDCARGO")),
                "instructions": "",
                "products": productos,
                "action_type": "PICK_UP",
                "location_type": "STORE"
            },
            {
                "products": [],
                "action_type": "DROP_OFF",
                "location_type": "CLIENT"
            }
        ],

        "client_info": {
            "email": convert_to_native(row.get("E_MAIL", "cliente@sodimac.com")),
            "phone": str(convert_to_native(row.get("FONO_CLI",""))),
            "first_name": first_name,
            "last_name": last_name,
            "address": convert_to_native(row.get("DIRECCION_CLI","")),          
            "lat": lat,
            "lng": lng,
            "complement": convert_to_native(row.get("OBSERVACION","")),
            "city": convert_to_native(row.get("CIUDAD_CLI","")),
            "comments": convert_to_native(row.get("OBSERVACION","")),
            "zip_code": str(convert_to_native(row.get("UBIGEO","")))
        }
    }

    return body

# ------------------------------------------------------------------------------------------------------------------
# 7. ENVÍO
# ------------------------------------------------------------------------------------------------------------------
def enviar(body):

    try:
        response = requests.post(API_URL, headers=HEADERS, json=body)

        print("\n================ RESPUESTA RAPPI ================")
        print("STATUS:", response.status_code)

        try:
            print("JSON:", response.json())
            return response.status_code, response.json()
        except:
            print("TEXT:", response.text)
            return response.status_code, response.text

    except Exception as e:
        print("Error:", e)
        return "ERROR", str(e)


# ------------------------------------------------------------------------------------------------------------------
# 8. GUARDAR HISTÓRICO
# ------------------------------------------------------------------------------------------------------------------
def guardar(resultados):

    if not resultados:
        return

    df_new = pd.DataFrame(resultados)

    existe = tabla_existe(
        bq_cliente,
        TABLE_ENVIOS
    )

    if not existe:

        job = (
            bq_cliente
            .load_table_from_dataframe(
                df_new,
                TABLE_ENVIOS,
                job_config=
                bigquery.LoadJobConfig(
                    schema=SCHEMA_ENVIOS,
                    write_disposition=
                    "WRITE_TRUNCATE"
                )
            )
        )

        job.result()

        print(
            f"Tabla de envíos creada con "
            f"{len(df_new)} registros"
        )

        return

    job = (
        bq_cliente
        .load_table_from_dataframe(
            df_new,
            TABLE_ENVIOS,
            job_config=
            bigquery.LoadJobConfig(
                schema=SCHEMA_ENVIOS,
                write_disposition=
                "WRITE_APPEND"
            )
        )
    )

    job.result()

    print(
        f"{len(df_new)} envíos agregados"
    )


# ------------------------------------------------------------------------------------------------------------------
# 9. FUNCIÓN PRINCIPAL
# ------------------------------------------------------------------------------------------------------------------
def ejecutar_step3():

    df = leer_base()

    if df.empty:
        return

    df = filtrar_listos(df)

    if df.empty:
        print(" Nada para enviar")
        return

    df = enriquecer(df)

    resultados = []

    for _, row in df.iterrows():

        print(f"\n Procesando reserva {row['NUM_RESERVA']}")

        lat, lng = limpiar_y_validar_coordenadas(
            row.get("LATITUD"),
            row.get("LONGITUD")
        )

        if lat is None or lng is None:

            direccion = limpiar_direccion(row)

            print(
                f" Buscando coordenadas: {direccion}"
            )

            lat, lng = obtener_coordenadas_google(
                direccion
            )

            if lat is None or lng is None:

                print(
                    f" No se pudo geocodificar "
                    f"reserva {row['NUM_RESERVA']}"
                )

                resultados.append({
                    "NUM_RESERVA": str(row["NUM_RESERVA"]),
                    "FECHA_ENVIO": datetime.now(),
                    "STATUS": "ERROR_GOOGLE",
                    "RESPONSE": None,
                    "PAYLOAD": None
                })

                continue

            lat = asegurar_precision(lat)
            lng = asegurar_precision(lng)

            print(
                f"Coordenadas obtenidas: "
                f"{lat}, {lng}"
            )

            df.loc[row.name, "LATITUD"] = lat
            df.loc[row.name, "LONGITUD"] = lng

            # TRUE = Google geocodificó
            df.loc[
                row.name,
                "FLAG_GEOCODIFICADO"
            ] = True

        else:

            lat = asegurar_precision(lat)
            lng = asegurar_precision(lng)

            # FALSE = ya existían coordenadas
            df.loc[
                row.name,
                "FLAG_GEOCODIFICADO"
            ] = False

        df.loc[row.name, "LATITUD"] = lat
        df.loc[row.name, "LONGITUD"] = lng

        row = df.loc[row.name]

        body = construir_payload(
            row,
            lat,
            lng
        )

        body = json.loads(
            json.dumps(
                body,
                default=str
            )
        )

        status, response_data = enviar(body)

        resultados.append({

            "NUM_RESERVA": str(row["NUM_RESERVA"]),

            "FECHA_ENVIO": datetime.now(),

            "STATUS": str(status),

            "PAYLOAD": json.dumps(
                body,
                default=str
            ),

            "RESPONSE": json.dumps(
                response_data,
                default=str
            )
        })

    guardar(resultados)

    # NUEVO
    actualizar_step2(df)

    print(
        "STEP2 ACTUALIZADO "
        "CON COORDENADAS"
    )

    print(
        "\nSTEP3 COMPLETADO"
    )
