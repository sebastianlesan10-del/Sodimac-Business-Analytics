#-------------------------------------------------------------------------------------------------
#--1. IMPORTAMOS LIBRERIAS NECESARIAS
#-------------------------------------------------------------------------------------------------
import os
import pyodbc
from pathlib import Path
from time import perf_counter
from dotenv import load_dotenv
import pandas as pd

print(pyodbc.drivers())

ruta_env = (
    Path(__file__)
    .resolve()
    .parent
    .parent
    / ".env"
)

load_dotenv(ruta_env)



def conectar_sql():
    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    username = os.getenv("SQL_USERNAME")
    password = os.getenv("SQL_PASSWORD")

    conn_str = (
        f"DRIVER={{SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password}"
    )
    return pyodbc.connect(conn_str)

#-------------------------------------------------------------------------------------------------
#--3. FUNCIÓN INSERTAR DF
#-------------------------------------------------------------------------------------------------
def insertar_dataframe(conn, df):
    
    df = df.where(
        pd.notnull(df),
        None
    )

    cursor = conn.cursor()

    query = """
    INSERT INTO Bd_Confirmacion_Estado_Reservas
    (
        SKU,
        Reserva,
        Motivo,
        Sub_Motivo,
        Responsable
    )
    VALUES
    (
        ?, ?, ?, ?, ?
    )
    """

    datos = list(
        df[
            [
                "SKU",
                "Reserva",
                "Motivo",
                "Sub_Motivo",
                "Responsable"
            ]
        ].itertuples(
            index=False,
            name=None
        )
    )

    inicio = perf_counter()

    print(
        "Cantidad registros:",
        len(datos)
    )


    cursor.executemany(
        query,
        datos
    )

    print(
        "Tiempo executemany:",
        perf_counter() - inicio
    )

    inicio = perf_counter()

    conn.commit()

    print(
        "Tiempo commit:",
        perf_counter() - inicio
    )

    cursor.close()


#-------------------------------------------------------------------------------------------------
#--4. ORQUESTADOR SQL
#-------------------------------------------------------------------------------------------------
def cargar_dataframe_sql(df):

    inicio = perf_counter()

    conn = conectar_sql()

    print(
        "Tiempo conexión:",
        perf_counter() - inicio
    )

    try:

        inicio = perf_counter()

        insertar_dataframe(
            conn,
            df
        )

        print(
            "Tiempo insertar_dataframe:",
            perf_counter() - inicio
        )

    finally:

        conn.close()
