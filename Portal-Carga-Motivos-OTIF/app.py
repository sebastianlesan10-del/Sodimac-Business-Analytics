from fastapi import FastAPI, Request, UploadFile
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates

from fastapi.staticfiles import StaticFiles

from Servicios.validaciones import validar_archivo
from Servicios.motivos import procesar_archivo

from Servicios.auditar import (
    auditoria_resumen,
    generar_excel_pendientes
)


import pandas as pd
from io import BytesIO
from time import perf_counter


app = FastAPI()

#----------------------------------------------------------------------------------------------------
#2. LE AVISAMOS AL SERVIDOR DONDE BUSCAR LOS PROGRAMAS ESTÁTICOS DE LA WEB
#---------------------------------------------------------------------------------------------------
app.mount(
    "/estaticos",
    StaticFiles(directory="estaticos"),
    name="estaticos"
)


templates = Jinja2Templates(directory="vistas")


@app.get("/")
def inicio(request: Request):

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"request": request}
    )

#----------------------------------------------------------------------------------------------------
#3. CREAMOS LOS ENDPOINTS PARA LOS ARCHIVOS QUE SE SUBIRÁN
#---------------------------------------------------------------------------------------------------

@app.post("/cargar_dadc")
async def cargar_dadc(archivo: UploadFile):

    try:

        inicio = perf_counter()

        contenido = await archivo.read()

        if archivo.filename.endswith(".xls"):

            df = pd.read_excel(
                BytesIO(contenido),
                engine="xlrd"
            )

        else:

            df = pd.read_excel(
                BytesIO(contenido),
                engine="openpyxl"
            )

        # VALIDACIONES EXISTENTES
        df = validar_archivo(
            archivo.filename,
            df
        )

        # NUEVO PROCESO BIGQUERY
        resultado = procesar_archivo(df)

        print(
            "Tiempo total:",
            perf_counter() - inicio
        )

        return resultado

    except Exception as e:

        print("TIPO:", type(e))
        print("ERROR:", repr(e))

        return {
            "estado": "error",
            "mensaje": str(e)
        }


@app.post("/cargar_rt")
async def cargar_rt(archivo: UploadFile):

    try:

        contenido = await archivo.read()

        if archivo.filename.endswith(".xls"):

            df = pd.read_excel(
                BytesIO(contenido),
                engine="xlrd"
            )

        else:

            df = pd.read_excel(
                BytesIO(contenido),
                engine="openpyxl"
            )

        df = validar_archivo(
            archivo.filename,
            df
        )

        resultado = procesar_archivo(df)

        return resultado

    except Exception as e:

        return {
            "estado": "error",
            "mensaje": str(e)
        }



@app.post("/cargar_dadpis")
async def cargar_dadpis(archivo: UploadFile):

    try:

        contenido = await archivo.read()

        if archivo.filename.endswith(".xls"):

            df = pd.read_excel(
                BytesIO(contenido),
                engine="xlrd"
            )

        else:

            df = pd.read_excel(
                BytesIO(contenido),
                engine="openpyxl"
            )

        df = validar_archivo(
            archivo.filename,
            df
        )

        resultado = procesar_archivo(df)

        return resultado

    except Exception as e:

        return {
            "estado": "error",
            "mensaje": str(e)
        }


#-----------------------------------------------------------
# ENDPOINT MODAL
#-----------------------------------------------------------
@app.get("/auditoria/{tipo}")
def auditar(tipo: str):

    print(
        f"AUDITORIA: {tipo}"
    )

    resultado = auditoria_resumen(
        tipo
    )

    print(
        "AUDITORIA OK"
    )

    return resultado

#-----------------------------------------------------------
# ENDPOINT EXCEL
#-----------------------------------------------------------
@app.get("/auditoria/excel/{tipo}")
def descargar_excel(tipo: str):

    archivo = generar_excel_pendientes(
        tipo
    )

    return FileResponse(
        archivo,
        filename=archivo
    )



print("APP CARGADA")

for ruta in app.routes:
    print(ruta.path)
