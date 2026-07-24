# ------------------------------------------------------------------------------------------------------------------
# 1. LIBRERÍAS
# ------------------------------------------------------------------------------------------------------------------
from prefect import flow, task

# IMPORTS DE STEPS
from Step1.Paso_1 import ejecutar_paso1
from Step2.Paso_2 import ejecutar_paso2
from Step3.Paso_3 import ejecutar_step3
from Step4.Paso_4 import ejecutar_step4


# ------------------------------------------------------------------------------------------------------------------
# 2. TASKS (CADA PASO COMO TAREA)
# ------------------------------------------------------------------------------------------------------------------

@task(name="Paso 1 - Extracción Oracle")
def paso1():
    ejecutar_paso1(debug=False)


@task(name="Paso 2 - Consolidación Reservas")
def paso2():
    ejecutar_paso2()


@task(name="Paso 3 - Envío Rappi")
def paso3():
    ejecutar_step3()

@task(name="Paso 4 - Archivo de tracking de pedidos")
def paso4():
    ejecutar_step4()

# ------------------------------------------------------------------------------------------------------------------
# 3. FLOW (ORQUESTADOR)
# ------------------------------------------------------------------------------------------------------------------
@flow(name="Pipeline Coordenadas Rappi")
def pipeline():

    print("===================================")
    print(" EJECUTANDO PIPELINE COMPLETO")
    print("===================================")

    paso1()
    paso2()
    paso3()
    paso4()

    print("===================================")
    print(" PIPELINE FINALIZADO")
    print("===================================")


# ------------------------------------------------------------------------------------------------------------------
# 4. EJECUCIÓN
# ------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    pipeline()