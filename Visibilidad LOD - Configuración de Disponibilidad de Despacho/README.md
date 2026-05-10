# 🚚 Lead Time & Delivery Coverage Optimization | Omnichannel Logistics Pipeline

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Google Cloud](https://img.shields.io/badge/GCP-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?style=for-the-badge&logo=googlebigquery&logoColor=white)
![Oracle](https://img.shields.io/badge/Oracle-F80000?style=for-the-badge&logo=oracle&logoColor=white)

## 📌 Contexto del Negocio
En el sector retail, la promesa de entrega (Lead Time) y la visibilidad de la cobertura a nivel nacional son críticos para la experiencia del cliente omnicanal. Este proyecto centraliza y automatiza la lógica de despachos, conectando la configuración de la base de datos transaccional con un entorno analítico en la nube para proyectar fechas de entrega reales.

**El reto:** El proceso original de seguimiento estaba inactivo ("muerto") desde hace más de dos meses. Las reglas de despacho dependían de procesos manuales aislados y de una arquitectura heredada, lo que generaba un punto ciego en la visibilidad logística operativa.

## 💡 La Solución
Se diseñó y desplegó un pipeline de datos *End-to-End* que extrae la configuración logística desde Oracle y la inyecta en Google BigQuery. Mediante transformaciones avanzadas en SQL, se calculan dinámicamente los calendarios de entrega y se integran en un dashboard centralizado. 

### ⚙️ Arquitectura de Datos
1. **Data Extraction (Python):** Conexión automatizada a Oracle DB mediante `oracledb`. Extracción de cruces entre Centros de Costo, capacidades y Zonas de despacho.
2. **Cloud Ingestion (GCP):** Uso de `google-cloud-bigquery` para actualizar la información diariamente en el *Data Warehouse* (BigQuery), estableciendo una única fuente de verdad.
3. **Data Transformation (Standard SQL):** - Homologación con maestros de división geográfica (Ubigeos).
   - **Lógica de Desplazamiento:** Algoritmo en SQL que lee matrices de días (ej. `LMWJVSD`), aplica operaciones de aritmética modular y calcula los días reales de despacho sumando el Lead Time (LT).
   - **Proyección de Fechas:** Generación de un array dinámico a 14 días para predecir la fecha mínima exacta del próximo despacho hábil.
4. **Data Visualization & BI:** Dashboard geoespacial interactivo para el monitoreo de capacidad, servicios (24H, 48H, Same Day) y horarios de corte.

## 📈 Impacto y Resultados
La reactivación y refactorización de este flujo de datos generó los siguientes resultados directos en la operación:

* **Modernización de Arquitectura:** Se eliminó por completo la dependencia indirecta de tableros heredados en Power BI, consolidando este pipeline como pieza clave en la migración estratégica del ecosistema de datos de Microsoft hacia Google Cloud Platform (GCP).
* **Recuperación y Gobernanza:** Se rescató y asumió el control total (end-to-end) de un proceso crítico inactivo, estableciendo por primera vez una documentación formal y estructurada para la gestión de la información.
* **Visibilidad Logística a Escala:** Se habilitó la proyección y mapeo activo de los tiempos de entrega en **1,380 comunas** a nivel nacional, asegurando el cumplimiento de la promesa al cliente.
* **Eficiencia Operativa:** Automatización total del flujo, eliminando los cuellos de botella manuales y ahorrando **10 horas-hombre al mes** en tareas operativas y de consolidación.

## 🧠 Core Engineering: Transformación de Fechas (SQL)
Para evitar bucles procedurales y dependencias complejas, la lógica de días hábiles se resolvió aplicando `UNNEST`, `REGEXP_EXTRACT_ALL` y `MOD` nativos de BigQuery. Esto permitió mapear y proyectar las ventanas de entrega de manera vectorizada, eficiente y altamente escalable directamente en el motor de la base de datos.
