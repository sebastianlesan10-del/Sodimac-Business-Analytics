# ------------------------------------------------------------------------------------------------------------------
# 1. LIBRERÍAS
# ------------------------------------------------------------------------------------------------------------------
import os
import pandas as pd
from datetime import datetime
import oracledb
from dotenv import load_dotenv

import sys
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound



# ------------------------------------------------------------------------------------------------------------------
# 2. CONFIGURACIÓN Y CONEXIÓN
# ------------------------------------------------------------------------------------------------------------------
load_dotenv()

ORACLE_CLIENT_PATH = os.getenv(
    "ORACLE_CLIENT_PATH"
)


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

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


SERVICE_ACCOUNT_FILE = (
    obtener_ruta_credenciales()
)

PROJECT_ID = os.getenv(
    "BQ_PROJECT_ID"
)

DATASET_ID = os.getenv(
    "BQ_DATASET_ID"
)

TABLE_STEP1 = (
    f"{PROJECT_ID}.{DATASET_ID}."
    f"RAPPI_Reservas_Sin_Coord"
)

SCHEMA_STEP1 = [

    bigquery.SchemaField("CANTIDAD", "FLOAT"),

    bigquery.SchemaField("CC_DESPACHA", "INTEGER"),
    bigquery.SchemaField("CC_ORIGEN", "INTEGER"),

    bigquery.SchemaField("REGION_CLI", "STRING"),
    bigquery.SchemaField("CIUDAD_CLI", "STRING"),
    bigquery.SchemaField("COMUNA_CLI", "STRING"),
    bigquery.SchemaField("COMUNA_DESP", "STRING"),

    bigquery.SchemaField("CORREO_CLI", "STRING"),
    bigquery.SchemaField("DIRECCION_CLI", "STRING"),
    bigquery.SchemaField("DIRECCION_DESP", "STRING"),
    bigquery.SchemaField("E_MAIL", "STRING"),

    bigquery.SchemaField("COD_EVENTO_B", "STRING"),
    bigquery.SchemaField("FECHA_BLOQUEADO", "STRING"),

    bigquery.SchemaField("COD_EVENTO_RB", "STRING"),
    bigquery.SchemaField("FECHA_RESERVADO_BODEGA", "STRING"),

    bigquery.SchemaField("COD_EVENTO_RT", "STRING"),
    bigquery.SchemaField("FECHA_EN_RUTA_TIENDA", "STRING"),

    bigquery.SchemaField("COD_EVENTO_PT", "INTEGER"),
    bigquery.SchemaField("FECHA_PREPARADO_TIENDA", "STRING"),

    bigquery.SchemaField("EXPRESS", "STRING"),

    bigquery.SchemaField("FONO_CLI", "STRING"),
    bigquery.SchemaField("FONO_DESP", "STRING"),
    bigquery.SchemaField("FONO2_DESP", "STRING"),

    bigquery.SchemaField("IDE", "STRING"),

    bigquery.SchemaField("LATITUD", "STRING"),
    bigquery.SchemaField("LONGITUD", "STRING"),

    bigquery.SchemaField("NOMBRE_CLI", "STRING"),
    bigquery.SchemaField("NOMBRE_DESP", "STRING"),

    bigquery.SchemaField("NUM_DOC", "STRING"),

    bigquery.SchemaField("NUM_ORDENVENTA", "STRING"),
    bigquery.SchemaField("NUM_ORDENVENTA2", "STRING"),

    bigquery.SchemaField("NUM_RESERVA", "STRING"),

    bigquery.SchemaField("OBSERVACION", "STRING"),

    bigquery.SchemaField("ORG_LVL_NUMBER", "STRING"),

    bigquery.SchemaField("PRD_LVL_NUMBER", "STRING"),

    bigquery.SchemaField("PRD_NAME_FULL", "STRING"),

    bigquery.SchemaField("PRECIO", "FLOAT"),

    bigquery.SchemaField("RANGO_HORARIO", "STRING"),

    bigquery.SchemaField("FECHA_ENTREGA_ORI", "DATETIME"),

    bigquery.SchemaField("FECHA_PROCESO", "DATETIME"),

    bigquery.SchemaField("KEY", "STRING")
]


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
# FUNCIÓN DE APOYO
# ------------------------------------------------------------------------------------------------------------------

def tabla_existe(cliente, table_ref):

    try:

        cliente.get_table(table_ref)

        return True

    except NotFound:

        return False



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
    
    WITH base AS (
    SELECT *
    FROM RESERVA_DTL d
    WHERE 
        d.FECHA_ENTREGA_ORI > TRUNC(SYSDATE)-1
        AND d.FECHA_ENTREGA_ORI < TRUNC(SYSDATE)+2
        AND d.CC_ORIGEN = 10       
        AND TO_NUMBER(SUBSTR(d.RANGO_HORARIO,2)) <=
            (
                FLOOR(
                    (TO_NUMBER(TO_CHAR(SYSDATE - (1/24), 'HH24')) - 9) / 2
                ) + 1
            )
        AND d.RANGO_HORARIO IN (
                    'E1','E2','E3',
                    'E4','E5','E6'
                )
    ),
    h AS (
  SELECT 
    CANAL_VENTA,
    LATITUD,
    LONGITUD,
    NUM_RESERVA,
    COD_ESTADOCAB,
    ORG_LVL_NUMBER,
    NUM_ORDENVENTA,
    TO_CHAR(FECHA_RESERVA,'DD.MM.YYYY HH24:MI:SS') FECHA_RESERVA,
    ID_VENDEDOR,NUM_DOC,TYPE_DOC,
    REGION_CLI,
    REGION_DESP,
    CIUDAD_CLI,
    COMUNA_CLI,
    NOMBRE_CLI,
    NOMBRE_DESP,
    DIRECCION_CLI,
    DIRECCION_DESP,
    COMUNA_DESP,
    FONO_CLI,
    FONO_DESP,
    FONO2_DESP,
    OBSERVACION,
    BLOQUEO,
    NUMRSV_PADRE,
    TIPO_RT,
    RT_PREPARADO,
    E_MAIL,
    MONTO_DESP,
    TIPO_RESERVA,
    FECHA_CREA,
    FECHA_CONFIRMA,
    TO_CHAR(FECHA_CREA,'DD.MM.YYYY HH24:MI:SS') FECHA_CREA_HORA,
    CORREO_CLI,
    ORDEN_VENTA_BASE,
    RESERVA_BASE,
    SITE_ID,
    IDE,
    IDE_DV
  FROM RESERVA_HDR
  )
    SELECT
      d.CANTIDAD, 
      d.CC_DESPACHA, 
      d.CC_ORIGEN, 
      h.REGION_CLI, 
      h.CIUDAD_CLI, 
      h.COMUNA_CLI, 
      h.COMUNA_DESP, 
      h.CORREO_CLI, 
      h.DIRECCION_CLI, 
      h.DIRECCION_DESP,
      h.E_MAIL,
      e1.COD_ESTADO COD_EVENTO_B,  
      TO_CHAR(e1.FECHA_EVENTO,
      'DD-MM-YYYY HH24:MI') FECHA_BLOQUEADO,
      e2.COD_ESTADO COD_EVENTO_RB, 
      TO_CHAR(e2.FECHA_EVENTO,
      'DD-MM-YYYY HH24:MI') FECHA_RESERVADO_BODEGA,
      e3.COD_ESTADO COD_EVENTO_RT, 
      TO_CHAR(e3.FECHA_EVENTO,
      'DD-MM-YYYY HH24:MI') FECHA_EN_RUTA_TIENDA,
      e4.COD_ESTADO COD_EVENTO_PT, 
      TO_CHAR(e4.FECHA_EVENTO,
      'DD-MM-YYYY HH24:MI') FECHA_PREPARADO_TIENDA,
      d.EXPRESS, 
      h.FONO_CLI, 
      h.FONO_DESP, 
      h.FONO2_DESP, 
      h.IDE, 
      h.LATITUD, 
      h.LONGITUD, 
      h.NOMBRE_CLI, 
      h.NOMBRE_DESP, 
      h.NUM_DOC, 
      h.NUM_ORDENVENTA,
      h.NUM_ORDENVENTA as NUM_ORDENVENTA2, 
      h.NUM_RESERVA, 
      h.OBSERVACION, 
      h.ORG_LVL_NUMBER, 
      d.PRD_LVL_NUMBER, 
      d.PRD_NAME_FULL, 
      d.PRECIO,
      d.RANGO_HORARIO, 
      d.FECHA_ENTREGA_ORI
    FROM base d
        LEFT JOIN h        
            ON h.NUM_RESERVA=d.NUM_RESERVA
        LEFT JOIN (SELECT NUM_RESERVA,SKU,ACTIVO FROM RESERVA_ST WHERE ACTIVO=1) ss
            ON ss.NUM_RESERVA=d.NUM_RESERVA AND ss.SKU=d.PRD_LVL_NUMBER
        LEFT JOIN EVENTO e1 
            ON e1.NUM_RESERVA=d.NUM_RESERVA 
                AND e1.NUM_DETALLE=d.NUM_DETALLE 
                AND e1.COD_ESTADO=300
        LEFT JOIN EVENTO e2 
            ON e2.NUM_RESERVA=d.NUM_RESERVA 
                AND e2.NUM_DETALLE=d.NUM_DETALLE 
                AND e2.COD_ESTADO=9
        LEFT JOIN EVENTO e3 
            ON e3.NUM_RESERVA=d.NUM_RESERVA 
                AND e3.NUM_DETALLE=d.NUM_DETALLE 
                AND e3.COD_ESTADO=112
        LEFT JOIN EVENTO e4 
            ON e4.NUM_RESERVA=d.NUM_RESERVA 
                AND e4.NUM_DETALLE=d.NUM_DETALLE 
                AND e4.COD_ESTADO=110
    WHERE e1.FECHA_EVENTO IS NULL
    AND e4.FECHA_EVENTO IS NOT NULL
    AND e3.FECHA_EVENTO IS NULL                       
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
# 6. HISTÓRICO
# ------------------------------------------------------------------------------------------------------------------
def guardar_step1(df):

    if df.empty:
        return

    df = df.copy()

    df["FECHA_PROCESO"] = (
        datetime.now()
        .replace(microsecond=0)
    )

    df["FECHA_ENTREGA_ORI"] = pd.to_datetime(
        df["FECHA_ENTREGA_ORI"],
        errors="coerce"
    )

    df["FECHA_PROCESO"] = pd.to_datetime(
        df["FECHA_PROCESO"],
        errors="coerce"
    )

    # --------------------------------------------------
    # CAMPOS STRING SEGÚN SCHEMA
    # --------------------------------------------------

    columnas_string = [

        "IDE",

        "NUM_ORDENVENTA",
        "NUM_ORDENVENTA2",

        "NUM_RESERVA",

        "ORG_LVL_NUMBER"

    ]

    for col in columnas_string:

        if col in df.columns:

            df[col] = (
                df[col]
                .fillna("")
                .astype(str)
            )

    # --------------------------------------------------
    # CAMPOS INTEGER SEGÚN SCHEMA
    # --------------------------------------------------

    columnas_integer = [

        "CC_DESPACHA",
        "CC_ORIGEN",
        "COD_EVENTO_PT",
        "CANTIDAD"

    ]

    for col in columnas_integer:

        if col in df.columns:

            df[col] = (
                pd.to_numeric(
                    df[col],
                    errors="coerce"
                )
                .fillna(0)
                .astype(int)
            )

    # --------------------------------------------------
    # KEY
    # --------------------------------------------------

    df["KEY"] = (
        df["NUM_RESERVA"]
        .astype(str)
        + "_"
        + df["PRD_LVL_NUMBER"]
        .astype(str)
    )

    existe = tabla_existe(
        bq_cliente,
        TABLE_STEP1
    )

    # --------------------------------------------------
    # CREAR TABLA
    # --------------------------------------------------

    if not existe:

        job = (
            bq_cliente
            .load_table_from_dataframe(
                df,
                TABLE_STEP1,
                job_config=
                bigquery.LoadJobConfig(
                    schema=SCHEMA_STEP1,
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

    # --------------------------------------------------
    # STAGING
    # --------------------------------------------------

    staging_table = (
        f"{PROJECT_ID}.{DATASET_ID}."
        f"RAPPI_Reservas_Sin_Coord_STG"
    )

    job = (
        bq_cliente
        .load_table_from_dataframe(
            df,
            staging_table,
            job_config=
            bigquery.LoadJobConfig(
                schema=SCHEMA_STEP1,
                write_disposition=
                "WRITE_TRUNCATE"
            )
        )
    )

    job.result()

    merge_sql = f"""

    MERGE `{TABLE_STEP1}` T

    USING `{staging_table}` S

    ON T.KEY = S.KEY

    WHEN NOT MATCHED THEN

    INSERT ROW

    """

    bq_cliente.query(
        merge_sql
    ).result()

    print(
        f"Step1 actualizado con "
        f"{len(df)} registros"
    )



# ------------------------------------------------------------------------------------------------------------------
# 7. EJECUTAR
# ------------------------------------------------------------------------------------------------------------------

def ejecutar_paso1(debug=False):

    conn = conectar_oracle()

    try:

        df = obtener_datos(conn)

        if debug:

            debug_dataframe(df)

        guardar_step1(df)

    finally:

        conn.close()

        print(
            "Conexión Oracle cerrada"
        )