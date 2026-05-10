"""
Pipeline de Extracción y Carga (ETL) - Lead Time y Cobertura
------------------------------------------------------------
Este script extrae la configuración de zonas y tiempos de despacho desde 
una base de datos Oracle (On-Premise) y actualiza diariamente una tabla 
destino en Google BigQuery (GCP).

ÍNDICE DEL SCRIPT (FLUJO DE EJECUCIÓN):
---------------------------------------
    0. IMPORTACIÓN DE LIBRERÍAS
       - Carga de dependencias nativas, de base de datos y de nube.
    1. CONFIGURACIÓN DE VARIABLES DE ENTORNO Y RUTAS
       - Lectura de credenciales (Oracle) y definición de paths locales.
    2. INICIALIZACIÓN DE CLIENTES (ORACLE Y GCP)
       - Arranque del cliente de Oracle y autenticación con BigQuery.
    3. DEFINICIÓN DE FUNCIONES CORE
       - get_oracle_connection(): Establece conexión a la BD.
       - get_oracle_df(): Ejecuta la query y devuelve un DataFrame.
    4. EJECUCIÓN DEL PIPELINE (MAIN)
       - Paso 4.1: Conexión y Extracción (Query a Oracle).
       - Paso 4.2: Backup Local (Guardado en CSV).
       - Paso 4.3: Carga a la Nube (Job a BigQuery con WRITE_TRUNCATE).
       - Paso 4.4: Cierre seguro de conexiones.
"""

# ==============================================================================
# 0. IMPORTACIÓN DE LIBRERÍAS
# ==============================================================================
import oracledb
import pandas as pd
import os
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import bigquery

# ==============================================================================
# 1. CONFIGURACIÓN DE VARIABLES DE ENTORNO Y RUTAS
# ==============================================================================
# Carga las credenciales de Oracle desde el archivo .env
load_dotenv() 

ODBMS_USER = os.getenv("ODBMS_USER")
ODBMS_PASS = os.getenv("ODBMS_PASS")
ODBMS_HOST = os.getenv("ODBMS_HOST")
ODBMS_PORT = os.getenv("ODBMS_PORT", "1521")
ODBMS_SID  = os.getenv("ODBMS_SID")

# Validación de seguridad: Asegura que todas las variables existan
for name, value in [
    ("ODBMS_USER", ODBMS_USER), ("ODBMS_PASS", ODBMS_PASS),
    ("ODBMS_HOST", ODBMS_HOST), ("ODBMS_PORT", ODBMS_PORT),
    ("ODBMS_SID",  ODBMS_SID)
]:
    if not value:
        raise RuntimeError(f"Variable de entorno faltante: {name}")

# Rutas a archivos locales (¡Reemplazar con rutas relativas en producción!)
ORACLE_CLIENT_DIR = os.getenv("ORACLE_CLIENT_DIR", r"./instantclient_11_2")
GCP_SERVICE_ACCOUNT = os.getenv("GCP_SERVICE_ACCOUNT", r"./credenciales/sa-logistic-sandbox.json")
BACKUP_CSV_PATH = r"./COMUNAZONACCVIEW.csv"

# ==============================================================================
# 2. INICIALIZACIÓN DE CLIENTES (ORACLE Y GCP)
# ==============================================================================
# Cliente Oracle
try:
    oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_DIR)
except Exception as e:
    print(f" Nota: Cliente Oracle ya inicializado o error en ruta: {e}")

# Cliente BigQuery
PROJECT_ID = "sod-pe-bi-sandbox"
credenciales = service_account.Credentials.from_service_account_file(GCP_SERVICE_ACCOUNT)
bq_cliente = bigquery.Client(credentials=credenciales, project=PROJECT_ID)

# ==============================================================================
# 3. DEFINICIÓN DE FUNCIONES CORE
# ==============================================================================

def get_oracle_connection():
    """
    Establece y retorna la conexión con la base de datos Oracle.
    """
    try:
        dsn = oracledb.makedsn(ODBMS_HOST, ODBMS_PORT, service_name=ODBMS_SID)
        connection = oracledb.connect(user=ODBMS_USER, password=ODBMS_PASS, dsn=dsn)
        connection.autocommit = False
        print(" Conexión a la base de datos Oracle establecida con éxito.")
        return connection
    except oracledb.DatabaseError as e:
        error, = e.args
        raise RuntimeError(f" Error conectando a Oracle: {error.message}")

def get_oracle_df(query, connection, params=None):
    """
    Ejecuta una consulta SQL en Oracle y devuelve los resultados como un DataFrame de Pandas.
    """
    print("⏳ Extrayendo datos de Oracle...")
    with connection.cursor() as cur:
        cur.execute(query, params or {})
        cols = [col[0] for col in cur.description]
        rows = cur.fetchall()
    print(f" Extracción completada. Filas obtenidas: {len(rows)}")
    return pd.DataFrame(rows, columns=cols)

# ==============================================================================
# 4. EJECUCIÓN DEL PIPELINE (MAIN)
# ==============================================================================
if __name__ == "__main__":
    
    # Abrir conexión
    conn = get_oracle_connection()

    # Query de extracción: Homologación de zonas, centros de costo y comunas
    query_extraccion = """
    WITH base_zona AS (
        SELECT
            ccosto,
            cod_zona,
            desc_zona,
            dias_despzona,
            capacidad
        FROM DADPEPR.ZONA
    ),
    cc_activo AS (
        SELECT
            org_lvl_number AS ccosto
        FROM DADPEPR.CENTRO_COSTO
        WHERE desactivado = 0
          AND despacha = 1
    )
    SELECT
        cc_comuna.canal_venta,
        cc_comuna.ccosto,
        cc_comuna.tiempo_desp,
        cc_comuna.cod_zona,
        cc_comuna.comuna,
        z.desc_zona,
        z.dias_despzona,
        z.capacidad
    FROM DADPEPR.CC_COMUNA cc_comuna
    LEFT JOIN base_zona z
        ON z.ccosto   = cc_comuna.ccosto
       AND z.cod_zona = cc_comuna.cod_zona
    INNER JOIN cc_activo c
        ON c.ccosto = cc_comuna.ccosto
    """

    # Extraer a DataFrame
    df_zonas = get_oracle_df(query_extraccion, conn)

    # Generar backup local en CSV
    df_zonas.to_csv(BACKUP_CSV_PATH, index=False, encoding="utf-8-sig")
    print(f" Backup local guardado en: {BACKUP_CSV_PATH}")

    # Definir destino en BigQuery
    DATASET_ID = "Omnicanal_Pe" 
    TABLE_ID = "CC_distrito_zona"
    TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Configurar Job de carga (Sobreescribir tabla completa)
    job_configuracion = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    # Cargar a GCP
    print(f" Iniciando carga a BigQuery en {TABLE_REF}...")
    job = bq_cliente.load_table_from_dataframe(
        df_zonas,
        TABLE_REF,
        job_config=job_configuracion
    )
    
    job.result()  # Espera a que termine la carga de datos
    print(f" Tabla {TABLE_REF} creada/reemplazada correctamente en GCP.")

    # Cerrar conexión de forma segura
    conn.close()
    print(" Conexión con Oracle cerrada.")
