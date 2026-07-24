# ------------------------------------------------------------------------------------------------------------------
# 1. LIBRERÍAS
# ------------------------------------------------------------------------------------------------------------------
import os
import oracledb
from dotenv import load_dotenv
import pandas as pd



pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)



# ------------------------------------------------------------------------------------------------------------------
# 2. CONFIGURACIÓN
# ------------------------------------------------------------------------------------------------------------------
load_dotenv()

ORACLE_CLIENT_PATH = r"C:\Users\jsantoses\OneDrive - Falabella\Escritorio\Procesos_Python\101.Conexion_BD\instantclient_11_2"


# ------------------------------------------------------------------------------------------------------------------
# 3. CONEXIÓN
# ------------------------------------------------------------------------------------------------------------------
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

    print("✅ Conectado a Oracle")
    return conn


# ------------------------------------------------------------------------------------------------------------------
# 4. VALIDAR SYSDATE
# ------------------------------------------------------------------------------------------------------------------
def validar_sysdate(conn):

    sql = """
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
    WHERE d.CC_ORIGEN IN (91)
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

    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    
    df = pd.read_sql(sql, conn)
    print(df)
    print("Columnas:")
    print(df.columns.tolist())

    #print("\n" + "="*60)
    #print("⏰ VALIDACIÓN COMPLETA DE ORACLE")
    #print("="*60)
    #print(f"FECHA COMPLETA : {row[0]}")
    #print(f"FECHA FORMATO  : {row[1]}")
    #print(f"HORA NUMERICA  : {row[2]}")
    #print(f"RANGO NUM      : {row[3]}")
    #print(f"RANGO FINAL    : {row[4]}")
    #print("="*60 + "\n")

# ------------------------------------------------------------------------------------------------------------------
# 5. MAIN
# ------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":

    conn = conectar_oracle()

    validar_sysdate(conn)


    conn.close()