# 🚚 Sistema de Análisis y Control de Costos de Flota
<img width="1050" height="717" alt="1 1" src="https://github.com/user-attachments/assets/f705938a-6cff-4f6f-a4b4-a75e9b672c57" />

## 📖 Resumen del Proyecto
Este proyecto consiste en un pipeline de datos desarrollado en SQL (Google BigQuery) y un Dashboard interactivo diseñado para la visibilidad, control y auditoría de los gastos logísticos de última milla (fletes y despachos). 

El sistema toma datos transaccionales, reservas de clientes y registros de rutas (vía Simpliroute), los limpia, los cruza aplicando lógicas de negocio complejas, y calcula el costo exacto por camión y por día.

## 🎯 Problema de Negocio
La operación logística generaba miles de registros diarios en sistemas separados (ERP, sistema de reservas y software de ruteo). Calcular el costo real de la flota requería procesar manualmente campos de texto libre, deduplicar transacciones, e identificar tarifas variables según:
* Capacidad del vehículo (Toneladas o Metros Cúbicos).
* Zona de entrega (Urbana, Lejana, Playa Sur, etc.).
* Necesidad de personal extra (Estibadores).

## 🛠️ Tecnologías Utilizadas
* **Lenguaje:** SQL (Dialecto Standard BigQuery).
* **Base de Datos / Data Warehouse:** Google BigQuery.
* **Técnicas SQL:** Common Table Expressions (CTEs), Window Functions (`ROW_NUMBER`, `AVG OVER`), Regular Expressions (`REGEXP_EXTRACT`, `REGEXP_CONTAINS`), Joins complejos, y Agregaciones de Arrays (`ARRAY_AGG`).
* **Visualización de Datos:** Looker Studio / Power BI (Dashboard).

## ⚙️ Arquitectura y Procesamiento de Datos (Pipeline ETL/ELT)
El script SQL realiza las siguientes fases de transformación:

1. **Extracción y Deduplicación:** Limpieza de la tabla de transacciones de ventas y reservas mediante funciones de ventana (`ROW_NUMBER`) para obtener la versión más reciente de cada registro.
2. **Normalización de Llaves y Extracción de Texto:** Uso intensivo de **RegEx** para transformar referencias de rutas y extraer el costo/precio ingresado manualmente por los asesores dentro de los campos de "Notas".
3. **Cálculo de Tarifarios Dinámicos:** Cruce con tablas de tarifarios históricos para estimar el costo esperado del flete basado en el peso de la línea y el destino.
4. **Enriquecimiento a Nivel de Camión:** Agrupación de la data por vehículo y fecha. Aquí se categorizan automáticamente los vehículos leyendo sus placas y descripciones para determinar su tipo (ej. "CAMIÓN 5TN", "CAMIÓN 13M3").
5. **Cálculo de Costo Final:** Aplicación de reglas de negocio y matrices de costos para definir la tarifa final a pagar al proveedor por cada ruta operada.

## 📊 Dashboard y KPIs Principales
La vista consolidada alimenta un panel interactivo que permite analizar métricas clave:
* **Gasto Total:** Monitoreo del presupuesto logístico invertido.
* **RxC (Reservas por Camión):** Indicador de eficiencia de la carga de los vehículos.
* **Costos por Zona:** Desglose del gasto entre Zonas Urbanas vs. Zonas Lejanas/Periféricas.
* **Evolutivo Temporal:** Análisis de tendencias diarias, semanales y mensuales para picos de demanda.
* **Detalle Operativo:** Tablas granulares a nivel de placa de vehículo y número de reserva para auditoría.
<img width="1038" height="788" alt="1 2" src="https://github.com/user-attachments/assets/57c077aa-c0fa-4c8a-b083-2e1f91a0ad31" />
<img width="1013" height="777" alt="1 3" src="https://github.com/user-attachments/assets/dfbf344c-0b7c-4209-9deb-5a6f2c015b34" />
<img width="1020" height="453" alt="1 4" src="https://github.com/user-attachments/assets/f66c66ed-50fb-4a08-a41a-312d1af85737" />



## 💻 Código Fuente
Puedes encontrar el pipeline completo de transformación de datos en el archivo [`costeo_flota.sql`](./costeo_flota.sql) dentro de este repositorio.
