# Centro de Mando CX: Ecosistema

## Impacto de Negocio
**Automatización Total:** Eliminé la preparación manual de reportes externos, centralizando flujos de datos en BigQuery.
**Monitoreo Real-Time:** Implementé un panel de autoservicio para visualizar KPIs críticos (NPS, NEC) sin intervención manual.
**Análisis Granular:** Capacidad de drill-down por canal, familia de producto, zona y rango horario para detectar ineficiencias operativas.

## Stack Técnico
**Lenguaje:** SQL (Google BigQuery).
**Lógica Avanzada:** Integración de +6 fuentes de datos (Medallia, Ventas, Planes corporativos) y procesamiento de texto (NLP) para tópicos y sentimientos

## Descripción
Este código es el motor del "Modelo Galaxia", una arquitectura que integra silos de información para ofrecer trazabilidad End-to-End
Permite monitorear la experiencia del cliente vinculada directamente a la transacción real, facilitando la toma de decisiones.

#Visualización del Panel (Ecosistema Modelo Galaxia)

El siguiente dashboard, desarrollado en Looker Studio, es la interfaz final que consume la arquitectura de datos detallada anteriormente. Permite una gestión proactiva de la experiencia del cliente mediante indicadores en tiempo real.

### 1. Vista General de Indicadores Compañía
<img width="1175" height="685" alt="1 " src="https://github.com/user-attachments/assets/1cf9a266-9736-43ac-b89d-a540a7720e5b" />
**Propósito**: Monitoreo de NPS por canales críticos (Tienda, VAD, VEE, C&D) frente al plan operativo.
**Impacto**: Centralización de más de 80,000 muestras para la toma de decisiones ejecutivas.

### 2. Análisis Segmentado por Canal y Metas
*Esta vista facilita la identificación de desviaciones tácticas respecto a las metas 2025-2026 establecidas por la organización.*

### 3. Trazabilidad del "Viaje del Cliente" (End-to-End)
<img width="870" height="517" alt="2 " src="https://github.com/user-attachments/assets/44c5af5e-65ed-4b68-83fd-1b3378b9187a" />

* **Propósito**: Identificar cuellos de botella específicos en cada etapa de la experiencia de compra (Tienda, Retiro, Despacho y Post-Venta).
* **Capacidad de Drill-down**: Análisis granular por atributos críticos como Infraestructura, Producto, Vendedor y Proceso de Pago.
* **Gestión de Post-Venta**: Monitoreo especializado del flujo de Cambios y Devoluciones para asegurar la retención y lealtad del cliente tras la compra.

### 4. Análisis de Sentimiento y Minería de Textos (NLP)
<img width="876" height="497" alt="3 " src="https://github.com/user-attachments/assets/878a41a3-9ee4-42ff-bca3-894b65545b3a" />

* **Propósito**: Transformar miles de comentarios abiertos en insights accionables mediante la clasificación automática de sentimientos y tópicos.
* **Funcionalidad**: Filtros dinámicos por sentimiento (Positivo/Negativo) y categorías temáticas (Logística, Tienda, Producto) para identificar causas raíz de detractación.
* **Impacto**: Visibilidad sobre la percepción cualitativa de +50,000 clientes, permitiendo priorizar mejoras en procesos de "Cajas" y "Tiempos de Entrega" basados en la voz directa del consumidor.

### 5. Benchmarking y Comportamiento Omnicanal (ROPO)
<img width="1677" height="852" alt="8" src="https://github.com/user-attachments/assets/aa7535c6-6001-4e76-87bc-d1761b792f9e" />

* **Comparativa de Banderas**: Monitoreo paralelo de NPS Sodimac vs. Maestro para análisis de posicionamiento competitivo.
* **Segmentación Estratégica**: Diferenciación de experiencia entre Cliente Hogar y Cliente Profesional para personalización de servicios.

<img width="1710" height="907" alt="32" src="https://github.com/user-attachments/assets/b9be1656-06f0-422b-9ff9-a38b0c84f07e" />

* **Impacto Digital en Tienda (ROPO)**: Medición del efecto de la investigación online en la satisfacción de compra física.
* **Análisis Geográfico**: Desglose de comportamiento ROPO por zonas y subregiones (Callao, Lima Norte, etc.) para optimizar estrategias de marketing local.
