# ------------------------------------------------------------------------------------------------------------------
# 1. LIBRERÍAS
# ------------------------------------------------------------------------------------------------------------------
import os
import pandas as pd
from datetime import datetime
import oracledb
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
import sys



# ------------------------------------------------------------------------------------------------------------------
# 2. CONFIGURACIÓN Y CONEXIÓN
# ------------------------------------------------------------------------------------------------------------------
load_dotenv()

ORACLE_CLIENT_PATH = os.getenv(
    "ORACLE_CLIENT_PATH"
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def conectar_oracle():
    oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_PATH)

    dsn = oracledb.makedsn(
        os.getenv("ODBMS_HOST"),
        os.getenv("ODBMS_PORT", "1531"),
        sid=os.getenv("ODBMS_SID")
    )

    conn = oracledb.connect(
        user=os.getenv("ODBMS_USER"),
        password=os.getenv("ODBMS_PASS"),
        dsn=dsn
    )

    print(" Conectado a Oracle")
    return conn


# ------------------------------------------------------------------------------------------------------------------
# 3. QUERY
# ------------------------------------------------------------------------------------------------------------------
def obtener_sql():
    return """
    SELECT
        h.NUM_RESERVA,
        PRD_LVL_NUMBER,
        PRD_NAME_FULL,
        CC_ORIGEN,
        CC_DESPACHA,
        PRD_KILO,
        PRD_M3,
        TIPO_STOCK,
        IDE_CHOFER,
        IDE_DV_CHOFER,
        NOMBRE_CHOFER,
        CANT_DESP,
        FECHA_REPARTO,
        FECHA_PREPARADO,
        FECHA_RESERVADO,
        FECHA_ENTREGADO,
        h.ORG_LVL_NUMBER,
        h.FECHA_RESERVA,
        h.REGION_DESP,
        h.CIUDAD_DESP,
        h.COMUNA_DESP,
        DIRECCION_DESP,
        NOMBRE_DESP,
        FONO_DESP,
        FONO2_DESP,
        OBSERVACION,
        NOMBRE_CLI,
        E_MAIL,
        CORREO_CLI,
        FONO_CLI,
        LATITUD,
        LONGITUD,
        MONTO_DESP,
        FECHA_CREA,
        FECHA_CONFIRMA,
        FECHA_DESPACHO,
        FECHA_DESPACHO_ORI,
        FECHA_DESPACHO2
    FROM RESERVA_DTL d
    LEFT JOIN RESERVA_HDR h
        ON h.NUM_RESERVA = d.NUM_RESERVA
    WHERE d.CC_ORIGEN IN (97)
        AND FECHA_DESPACHO >= TRUNC(SYSDATE)+1
        AND FECHA_DESPACHO <  TRUNC(SYSDATE)+2
        AND COMUNA_DESP IN ('VILLA EL SALVADOR',
                            'SAN ANTONIO',
                            'PUCUSANA',
                            'SAN BARTOLO',
                            'CHILCA',
                            'PUNTA NEGRA',
                            'LURIN',
                            'ASIA',
                            'PUNTA HERMOSA',
                            'CERRO AZUL',
                            'SANTA MARIA DEL MAR',
                            'MALA',
                            'VENTANILLA',
                            'PUENTE PIEDRA',
                            'CHACLACAYO',
                            'LURIGANCHO',
                            'ANCON',
                            'MI PERU',
                            'SANTA ROSA',
                            'PACHACAMAC',
                            'CIENEGUILLA',
                            'CARABAYLLO',
                            'SANTA ANITA',
                            'LOS OLIVOS',
                            'COMAS',
                            'SAN JUAN DE LURIGANCHO',
                            'SAN JUAN DE MIRAFLORES',
                            'INDEPENDENCIA',
                            'SAN MARTIN DE PORRES')
        AND (
            h.LATITUD IS NULL
            OR h.LONGITUD IS NULL
            OR TRIM(h.LATITUD) IN ('', '0', '0.0')
            OR TRIM(h.LONGITUD) IN ('', '0', '0.0')
        )
    """


# ------------------------------------------------------------------------------------------------------------------
# 4. EJECUTAR QUERY
# ------------------------------------------------------------------------------------------------------------------
def obtener_datos(conn):
    sql = obtener_sql()

    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=cols)

    print(f" Filas obtenidas: {len(df)}")
    return df


# ------------------------------------------------------------------------------------------------------------------
# 5. DEBUG / VISUALIZACIÓN
# ------------------------------------------------------------------------------------------------------------------
def debug_dataframe(df):

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    print("\n" + "="*80)
    print("INFO GENERAL")
    print("="*80)
    print(df.info())

    print("\nCOLUMNAS:")
    print(df.columns.tolist())

    print("\nPRIMERAS FILAS:")
    print(df.head(10))

    print("\nÚLTIMAS FILAS:")
    print(df.tail(10))


# ------------------------------------------------------------------------------------------------------------------
# 5.5. BIGQUEY
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
TABLE_ID = "TRACKER_Reservas_Sin_Coord"
TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
print(f" BigQuery destino: {TABLE_REF}")
TABLE_TEMP = TABLE_REF + "_temp"
SCHEMA = [

    bigquery.SchemaField("KEY", "STRING"),

    bigquery.SchemaField("NUM_RESERVA", "INT64"),
    bigquery.SchemaField("PRD_LVL_NUMBER", "STRING"),
    bigquery.SchemaField("PRD_NAME_FULL", "STRING"),

    bigquery.SchemaField("CC_ORIGEN", "INT64"),
    bigquery.SchemaField("CC_DESPACHA", "INT64"),

    bigquery.SchemaField("PRD_KILO", "FLOAT64"),
    bigquery.SchemaField("PRD_M3", "FLOAT64"),

    bigquery.SchemaField("TIPO_STOCK", "STRING"),

    bigquery.SchemaField("IDE_CHOFER", "INT64"),
    bigquery.SchemaField("IDE_DV_CHOFER", "STRING"),
    bigquery.SchemaField("NOMBRE_CHOFER", "STRING"),

    bigquery.SchemaField("CANT_DESP", "INT64"),

    bigquery.SchemaField("FECHA_REPARTO", "DATETIME"),
    bigquery.SchemaField("FECHA_PREPARADO", "DATETIME"),
    bigquery.SchemaField("FECHA_RESERVADO", "DATETIME"),
    bigquery.SchemaField("FECHA_ENTREGADO", "DATETIME"),

    bigquery.SchemaField("ORG_LVL_NUMBER", "INT64"),

    bigquery.SchemaField("FECHA_RESERVA", "DATETIME"),

    bigquery.SchemaField("REGION_DESP", "STRING"),
    bigquery.SchemaField("CIUDAD_DESP", "STRING"),
    bigquery.SchemaField("COMUNA_DESP", "STRING"),
    bigquery.SchemaField("DIRECCION_DESP", "STRING"),
    bigquery.SchemaField("NOMBRE_DESP", "STRING"),
    bigquery.SchemaField("FONO_DESP", "STRING"),
    bigquery.SchemaField("FONO2_DESP", "STRING"),
    bigquery.SchemaField("OBSERVACION", "STRING"),
    bigquery.SchemaField("NOMBRE_CLI", "STRING"),
    bigquery.SchemaField("E_MAIL", "STRING"),
    bigquery.SchemaField("CORREO_CLI", "STRING"),
    bigquery.SchemaField("FONO_CLI", "STRING"),

    bigquery.SchemaField("LATITUD", "STRING"),
    bigquery.SchemaField("LONGITUD", "STRING"),

    bigquery.SchemaField("MONTO_DESP", "FLOAT64"),

    bigquery.SchemaField("FECHA_CREA", "DATETIME"),
    bigquery.SchemaField("FECHA_CONFIRMA", "DATETIME"),
    bigquery.SchemaField("FECHA_DESPACHO", "DATETIME"),
    bigquery.SchemaField("FECHA_DESPACHO_ORI", "DATETIME"),
    bigquery.SchemaField("FECHA_DESPACHO2", "DATETIME"),

    bigquery.SchemaField("FECHA_PROCESO", "DATETIME")
]

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE
    )
bq_cliente = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
    )



#------------------------------------------------------------------------------------------------------------------
# FUNCIÓN PARA DETERMINAR SI EXISTE O NO LA TABLA DE BIGQUERY
#------------------------------------------------------------------------------------------------------------------
def tabla_existe(cliente, table_ref):
    try:
        cliente.get_table(table_ref)
        return True
    except NotFound:
        return False



# ------------------------------------------------------------------------------------------------------------------
# 6. HISTÓRICO
# ------------------------------------------------------------------------------------------------------------------
def guardar_historico_bigquery(df):

    df = df.copy()   
    df["KEY"] = (
        df["NUM_RESERVA"].astype(str).str.strip()
        + "_"
        + df["PRD_LVL_NUMBER"].astype(str).str.strip()
    )
    df["FECHA_PROCESO"] = datetime.now().replace(microsecond=0)


    existe = tabla_existe(bq_cliente, TABLE_REF)

    # ---------------------------------------------------------
    # PRIMERA CARGA
    # ---------------------------------------------------------

    if not existe:

        print(" Tabla no existe. Creando tabla...")

        job = bq_cliente.load_table_from_dataframe(
            df,
            TABLE_REF,
            job_config=bigquery.LoadJobConfig(
                schema=SCHEMA,
                write_disposition="WRITE_TRUNCATE"
            )
        )

        job.result()

        print(f" Tabla creada con {len(df)} registros")
        return

    # ---------------------------------------------------------
    # CARGA TEMPORAL
    # ---------------------------------------------------------

    job_temp = bq_cliente.load_table_from_dataframe(
        df,
        TABLE_TEMP,
        job_config=bigquery.LoadJobConfig(
            schema=SCHEMA,
            write_disposition="WRITE_TRUNCATE"
        )
    )

    job_temp.result()

    print(" Tabla temporal cargada")

    # ---------------------------------------------------------
    # MERGE
    # MISMA LÓGICA DEL EXCEL:
    # NUM_RESERVA + PRD_LVL_NUMBER
    # ---------------------------------------------------------

    merge_sql = f"""
    MERGE `{TABLE_REF}` T
    USING `{TABLE_TEMP}` S

    ON T.KEY = S.KEY

    WHEN NOT MATCHED THEN
    INSERT ROW
    """

    bq_cliente.query(merge_sql).result()

    print(" Nuevos registros agregados")

    # ---------------------------------------------------------
    # LIMPIEZA
    # ---------------------------------------------------------

    bq_cliente.delete_table(TABLE_TEMP, not_found_ok=True)

    print(" Tabla temporal eliminada")


# ------------------------------------------------------------------------------------------------------------------
# 7. FUNCIÓN PRINCIPAL (LO QUE IMPORTA EXPORTAR)
# ------------------------------------------------------------------------------------------------------------------
def ejecutar_paso1(debug=True):

    conn = conectar_oracle()

    try:
        df = obtener_datos(conn)

        if df.empty:
            print("⚠️ No hay datos")
            return df

        if debug:
            debug_dataframe(df)

        guardar_historico_bigquery(df)

        return df

    finally:
        conn.close()
        print(" Conexión Oracle cerrada")