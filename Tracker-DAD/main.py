from prefect import flow

from Step1.Paso_1 import ejecutar_paso1
from Step2.Paso_2 import ejecutar_paso2
from Step3.Paso_3 import ejecutar_paso3
from Step4.Paso_4 import ejecutar_step4


# -----------------------------------------------------------
# CORTE
# 11:00 / 13:00 / 16:00 / 18:00
# -----------------------------------------------------------
@flow(name="Tracker Corte")
def tracker_corte():

    print("INICIANDO CORTE")

    ejecutar_paso1()
    ejecutar_paso2()
    ejecutar_paso3()

    print("CORTE FINALIZADO")


# -----------------------------------------------------------
# CAPTURA
# 12:00 / 14:00 / 15:00 / 17:00 / 18:10
# -----------------------------------------------------------
@flow(name="Tracker Captura")
def tracker_captura():

    print("INICIANDO CAPTURA")

    ejecutar_paso3()

    print("CAPTURA FINALIZADA")


# -----------------------------------------------------------
# ALERTA FINAL
# 18:15
# -----------------------------------------------------------
@flow(name="Tracker Alerta")
def tracker_alerta():

    print("INICIANDO ALERTA")

    ejecutar_step4()

    print("ALERTA FINALIZADA")