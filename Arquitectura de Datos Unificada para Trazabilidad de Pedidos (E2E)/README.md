# 🎯Motor de Trazabilidad Logística y Experiencia del Cliente (CX)

## 📖 Resumen del Proyecto
Este proyecto soluciona un desafío clásico en operaciones omnicanal: **conectar el sentimiento del cliente con la realidad logística**. 

El "Nexu-OTIF" es una arquitectura de datos desarrollada en Google BigQuery que unifica la base de encuestas de satisfacción del cliente (Medallia) con la tabla de cumplimiento operativo de despacho (TRF OTIF - On Time In Full), preservando estrictamente la integridad estadística de las encuestas (universo VAD: Despacho a Domicilio y Retiro en Tienda).
<img width="1008" height="571" alt="2 1" src="https://github.com/user-attachments/assets/1cc78cb2-02e2-4be5-8a55-6b3a17670f43" />


## 🧩 El Problema de Negocio y la Solución Técnica
* **El Problema:** Un cliente responde una sola encuesta por compra, pero su compra puede tener múltiples líneas de productos, despachadas desde diferentes orígenes y en distintas fechas. Un `JOIN` tradicional entre estas bases genera una "explosión de filas", duplicando respuestas de NPS y arruinando las métricas.
* **La Solución:** Se implementó una lógica de gestión de cardinalidad mediante Expresiones Regulares (Regex) para limpiar las llaves de cruce, y funciones de ventana (`ROW_NUMBER()`) para priorizar la fila logística más crítica (ej. el artículo con el peor retraso). Así, se logra un match perfecto de `1 a 1` o `1 a 0`, protegiendo la base de Medallia.

## 🛠️ Tecnologías y Técnicas Aplicadas
* **Data Warehouse:** Google BigQuery.
* **Técnicas SQL Avanzadas:** * Limpieza de documentos con `REGEXP_REPLACE`.
  * Control de cardinalidad con `ROW_NUMBER() OVER (PARTITION BY...)`.
  * Subconsultas y CTEs (`WITH`) para organizar la ingesta lógica.
  * Lógica condicional anidada (`COALESCE`, `CASE WHEN`, `IF`) para determinar el cumplimiento de la promesa.
* **Visualización de Datos:** Looker Studio / Power BI.

## 📊 Insights y Dashboard (CX Center)
El cruce de estas bases alimenta un dashboard diseñado para responder preguntas estratégicas:
1. **Impacto del Incumplimiento:** ¿Cuántos puntos de NPS perdemos exactamente por cada día de retraso en la entrega?
2. **Fricción Operativa:** ¿Cómo afecta al CSAT que un cliente reciba su pedido en entregas fragmentadas (fechas distintas) vs. un pedido consolidado?
3. **Desempeño por Nodo:** Evaluación del NPS según el centro de distribución de origen (Bodega vs. Tienda) y la región de despacho.

### Vistas del Dashboard
<img width="1020" height="802" alt="2 2" src="https://github.com/user-attachments/assets/35f960cf-b382-4789-a1f2-027520bbc6f9" />
<img width="991" height="841" alt="2 3" src="https://github.com/user-attachments/assets/55f3ba85-6f91-4b6d-8794-6481da7c1b98" />
<img width="997" height="862" alt="2 4" src="https://github.com/user-attachments/assets/ed15f684-40c9-44b8-a97f-03f7c4f12974" />
<img width="991" height="457" alt="2 5" src="https://github.com/user-attachments/assets/41640d10-efa5-4c4e-9f40-f6fee75873a6" />



## 💻 Código Fuente
La lógica de normalización y el join principal se encuentran documentados en el archivo [`nexu_otif_logistics_cx_engine.sql`](./nexu_otif_logistics_cx_engine.sql).

