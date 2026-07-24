#-------------------------------------------------------------------------------------------------
#--1. IMPORTAMOS LIBRERIAS NECESARIAS
#-------------------------------------------------------------------------------------------------
import pandas as pd
from io import BytesIO

#-------------------------------------------------------------------------------------------------
#--2. CONSTRUIMOS LAS FUNCIONES DE VALIDACION
#-------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------
#-- VALIDA QUE EL ARCHIVO SUBIDO SEA UN EXCEL
#-------------------------------------------------------------------------------------------------
def validar_extension(nombre_archivo):
    nombre_archivo = nombre_archivo.lower()
    if not nombre_archivo.endswith(("xls","xlsx")):
        raise ValueError(
            "Solo se permiten archivos Excel"
        )

#-------------------------------------------------------------------------------------------------
#--VALIDA QUE EL ARCHIVO CARGADO CONTENGA REGISTROS
#-------------------------------------------------------------------------------------------------
def validar_archivo_vacio(df):
    if df.empty:
        raise ValueError(
            "El archivo no contiene registros"
        )

#-------------------------------------------------------------------------------------------------
#--VALIDA QUE EL ARCHIVO CARGADA TENGA LA ESTRUCTURA MINIMA REQUERIDA PARA EL PROCESO
#-------------------------------------------------------------------------------------------------
def validar_columnas(df):
    columnas_obligatorias={
        "SKU",
        "Reserva",
        "Motivo",
        "Sub_Motivo",
        "Responsable"
    }

    faltantes = (
        columnas_obligatorias - set(df.columns)
    )

    if faltantes:
        raise ValueError(
            f"Columnas faltantes:{list(faltantes)}"
        )
#-------------------------------------------------------------------------------------------------
#-- CONSERVA SOLO LAS COLUMNAS NECESARIAS PARA EL PROCESO
#-------------------------------------------------------------------------------------------------
def seleccionar_columnas_requeridas(df):

    columnas_requeridas = [
        "SKU",
        "Reserva",
        "Motivo",
        "Sub_Motivo",
        "Responsable"
    ]

    return df[columnas_requeridas].copy()
#-------------------------------------------------------------------------------------------------
#--VALIDA QUE NO EXISTAN FILAS VACIAS QUE PUEDAN CARGARSE POR ERRO
#-------------------------------------------------------------------------------------------------
def eliminar_filas_vacias(df):
    return df.dropna(
        how="all"
    ).reset_index(drop=True)

#-------------------------------------------------------------------------------------------------
#--VALIDA EL MÍNIMO DE CAMPOS NECESARIOS QUE DEBEN CONTENER INFORMACIÓN PARA CONSIDERARSE UNA FILA
#-- QUE SE CONSIDERE ADECUADA PARA EL PROCESO
#-------------------------------------------------------------------------------------------------
def eliminar_filas_sin_datos_clave(df):
    columnas_clave = [
        "SKU",
        "Reserva",
        "Motivo"
    ]

    return df.dropna(
        subset=columnas_clave,
        how="all"
    ).reset_index(drop=True)


#-------------------------------------------------------------------------------------------------
#--3. DISEÑAMOS EL ORQUESTADOR DE VALIDACIONES
#-------------------------------------------------------------------------------------------------
def validar_archivo(nombre_archivo,df):
    
    validar_extension(nombre_archivo)
    validar_archivo_vacio(df)
    validar_columnas(df)    
# NUEVO
    df = seleccionar_columnas_requeridas(df)
    df = eliminar_filas_vacias(df)
    df = eliminar_filas_sin_datos_clave(df)
    validar_archivo_vacio(df)
    return df