# 🚀 Transformación del Proceso OTIF

> Automatización del proceso de carga, validación y actualización de indicadores OTIF mediante una arquitectura basada en **Python** y **Google Cloud Platform**.

📄 **Presentación Ejecutiva:** [Proyecto-OTIF-Agil.pdf](https://github.com/user-attachments/files/30620143/Proyecto-OTIF-Agil.pdf)


---

# 📌 Descripción

Este proyecto nace con el objetivo de transformar un proceso operativo crítico para la actualización del indicador **OTIF**, reemplazando un flujo manual y dependiente de múltiples áreas por una solución completamente automatizada.

La solución incorpora un **portal web de carga**, procesamiento automático de archivos, integración con **Google Cloud Platform** y actualización casi en tiempo real de los dashboards utilizados para el seguimiento operativo.

---

# 🎯 Problema

El proceso original presentaba diversos desafíos operativos:

- Dependencia entre distintas áreas para iniciar el procesamiento.
- Validaciones manuales antes de ejecutar la carga.
- Reprocesos frecuentes ante cualquier inconsistencia.
- Tiempos de espera elevados.
- Actualización tardía de los indicadores.
- Mayor riesgo de errores operativos.

Como consecuencia, la información utilizada para la toma de decisiones no siempre estaba disponible de forma oportuna.

---

# 💡 Solución Implementada

Se diseñó una arquitectura automatizada que permite procesar la información desde su carga hasta la actualización de los indicadores sin intervención manual.

El flujo general del proceso es el siguiente:

1. El usuario carga el archivo correspondiente desde el portal web.
2. El sistema valida automáticamente la información recibida.
3. Los datos son procesados mediante servicios desplegados en Google Cloud.
4. La información es almacenada en BigQuery.
5. Los dashboards se actualizan automáticamente.
6. Si existen registros pendientes, el sistema genera un reporte para su revisión y corrección.

---

# 🖥️ Portal de Carga

El portal permite que cada área cargue sus archivos de forma independiente y supervise el estado del procesamiento en tiempo real.

<img width="1893" height="978" alt="portal-1" src="https://github.com/user-attachments/assets/0574166b-1525-4d70-881c-d45bf4c82d50" />


Características principales:

- Carga independiente por tipo de archivo.
- Seguimiento del progreso del procesamiento.
- Visualización del estado de cada etapa.
- Auditoría automática de registros pendientes.

---

# 🔍 Auditoría Automática

Cuando el sistema detecta registros pendientes, genera automáticamente un listado para facilitar su revisión y posterior corrección.

<img width="1867" height="947" alt="portal-2" src="https://github.com/user-attachments/assets/e6366def-c3a4-4aee-8591-d05416983a2a" />


Además, el usuario puede descargar el detalle en formato Excel para realizar el análisis correspondiente.

---

# 📊 Dashboard de Indicadores

Una vez finalizado el procesamiento, los indicadores OTIF se actualizan automáticamente.

<img width="1133" height="836" alt="portal-3" src="https://github.com/user-attachments/assets/64499d28-7865-4cf2-b0ce-c91daa58f70f" />


El dashboard permite monitorear indicadores clave y realizar seguimiento al desempeño operativo prácticamente en tiempo real.

---

# 📈 Análisis de Información

La solución también proporciona el detalle de reservas, motivos, submotivos y demás información necesaria para el análisis operativo.

<img width="1132" height="758" alt="portal-4" src="https://github.com/user-attachments/assets/843e446b-aacd-4030-8bde-4d0bb17a7fc7" />


---

# ⚡ Resultados Obtenidos

| Indicador | Antes | Después |
|-----------|--------|----------|
| Tiempo de procesamiento | 15 - 25 minutos | 15 - 30 segundos |
| Validaciones | Manuales | Automáticas |
| Dependencia entre áreas | Alta | Baja |
| Reprocesos | Frecuentes | Mínimos |
| Actualización del Dashboard | Diferida | Casi en tiempo real |

---

## 🛠️ Tecnologías Utilizadas

<p align="left">

<img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" />

<img src="https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white" />

<img src="https://img.shields.io/badge/HTML5-E34F26?style=for-the-badge&logo=html5&logoColor=white" />

<img src="https://img.shields.io/badge/CSS3-1572B6?style=for-the-badge&logo=css3&logoColor=white" />

<img src="https://img.shields.io/badge/JavaScript-F7DF1E?style=for-the-badge&logo=javascript&logoColor=black" />

<img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" />

<img src="https://img.shields.io/badge/Google_Cloud_Run-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white" />

<img src="https://img.shields.io/badge/BigQuery-669DF6?style=for-the-badge&logo=googlebigquery&logoColor=white" />

<img src="https://img.shields.io/badge/Looker_Studio-4285F4?style=for-the-badge&logo=looker&logoColor=white" />

<img src="https://img.shields.io/badge/Git-F05032?style=for-the-badge&logo=git&logoColor=white" />

<img src="https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white" />

</p>

---

## 📂 Estructura del Proyecto

```text
Portal-Carga-Motivos-OTIF/
│
├── Servicios/                  # Lógica de negocio y procesamiento
│   ├── auditar.py              # Auditoría de registros pendientes
│   ├── motivos.py              # Procesamiento de motivos OTIF
│   ├── sql.py                  # Consultas a BigQuery
│   ├── transformaciones.py     # Transformación y limpieza de datos
│   └── validaciones.py         # Validaciones de archivos y reglas de negocio
│
├── estaticos/
│   ├── estilos.css             # Estilos de la aplicación
│   ├── funciones.js            # Funciones del lado del cliente
│   └── imagenes/               # Recursos gráficos
│
├── vistas/
│   └── index.html              # Interfaz principal del portal
│
├── Dockerfile                  # Contenedor para despliegue en Cloud Run
│
└── README.md
```

---

# 📑 Documentación

Para conocer el análisis completo del proyecto, la arquitectura propuesta y los resultados obtenidos, consulta la presentación ejecutiva.

📄 **[Presentación Ejecutiva](https://github.com/user-attachments/files/30620221/Proyecto-OTIF-Agil.pdf)**

---

# 👨‍💻 Autor

**Sebastián Santos**

Ingeniero de Datos | Automatización | Python | Google Cloud Platform

- LinkedIn: www.linkedin.com/in/jose-sebastian-santos-espinoza-data
