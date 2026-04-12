# Sodtrack Intelligence Engine: Ecosistema de Servicios de Instalación 🛠️📊

## Impacto de Negocio
- **Trazabilidad de Proyectos 360°:** Desarrollé una arquitectura que identifica el estado real de los proyectos (Presupuestado vs. Ejecutado), permitiendo a la gerencia monitorear el pipeline de ventas de servicios.
- **Normalización de Claves Canónicas:** Resolví el desafío de cruce de datos entre sistemas externos (Boletas/Facturas) y encuestas de satisfacción (Medallia) mediante lógica avanzada de Regex, logrando una tasa de match sin precedentes.
- **Auditoría de Procesos:** Visibilidad total sobre el ciclo de vida del servicio: desde la creación del presupuesto hasta la finalización por el contratista y el NPS del cliente.

## Descripción Técnica
Este pipeline de datos en BigQuery procesa el log de eventos de **Sodtrack** para reconstruir la historia de cada servicio. El código implementa una modulación compleja para evitar la duplicidad de registros y asegurar que cada métrica se asigne al actor correcto (Cliente, Contratista o Administrador).

### Innovaciones Técnicas:
1. **Modulación por Funciones de Ventana:** Uso intensivo de `ROW_NUMBER()` y `PARTITION BY` para deducir estados iniciales (`created`) y finales (`done`) de cada reserva.
2. **Matching mediante Regex Avanzado:** Implementación de un extractor dinámico para identificar tipos de documentos (B/F) y normalizar números con *padding* (12 dígitos), permitiendo el cruce con silos de datos externos.
3. **Cálculo de Budget Execution:** Integración de lógica de presupuestos para diferenciar servicios "Presupuestados" de los "Ejecutados" a nivel de Proyecto ID.

## Stack Técnico
- **Entorno:** Google BigQuery (Standard SQL).
- **Lógica:** CTEs Multinivel, Expresiones Regulares (Regex), Window Functions, Normalización de Strings y Lógica de Negocio Condicional.

### Visualización Operativa: Panel de Control Sodtrack
Este dashboard es el resultado final del procesamiento de logs y eventos, diseñado para la toma de decisiones estratégicas sobre la red de servicios e instalaciones.

- **Monitoreo de Eficiencia**: Visualización en tiempo real de la tasa de ejecución (66.1%) y el volumen de servicios gestionados.
- **Gestión de Calidad y Fallas**: Ranking detallado de incidencias que permite identificar causas raíz de fricción operativa, como daños de producto o inasistencias.
- **Análisis por Categoría**: Identificación de servicios críticos por tasa de incidencia para la optimización de protocolos de instalación.

<img width="1220" height="831" alt="1 " src="https://github.com/user-attachments/assets/fe8fe5a4-e2f3-4a0a-8638-a0570a87979f" />
<img width="1191" height="803" alt="3 " src="https://github.com/user-attachments/assets/4e830022-7610-4779-911f-1c282ecea459" />
<img width="1202" height="843" alt="4 " src="https://github.com/user-attachments/assets/0e6db165-9126-41ce-84cc-f329cbcca08b" />

### Auditoría de Proveedores y Control de SLAs (Sodtrack)
<img width="1196" height="718" alt="6 " src="https://github.com/user-attachments/assets/d2423216-4731-4346-b685-21810ecf0f2b" />

- **Gestión de SLAs**: Monitoreo del nivel de servicio (SLA 3 días) y tasa de promesa cumplida para el control de contratistas externos.
- **Optimización de Tiempos de Ciclo**: Análisis evolutivo del tiempo de agendamiento y ejecución para la reducción de fricción en la última milla.

<img width="1195" height="831" alt="7 " src="https://github.com/user-attachments/assets/85bc4da7-3374-4dae-9d96-8a86cc4185d1" />

- **Trazabilidad Individual**: Capacidad de auditoría a nivel de número de reserva, permitiendo el seguimiento de estados operativos críticos.
- **Eficiencia por Canal y Tienda**: Comparativa de tasas de ejecución y cancelación entre Tienda Física vs. Virtual para la toma de decisiones localizadas.

### Correlación de Calidad y Cumplimiento Logístico
<img width="1197" height="861" alt="12" src="https://github.com/user-attachments/assets/752bca4d-b07d-4c71-832f-1daa8effd62a" />

- **Análisis de Fidelidad**: Cuantificación del impacto de la logística en la lealtad del cliente, demostrando una caída del 68% en el NPS ante retrasos mayores a una semana.
- **Auditoría de Atributos de Servicio**: Monitoreo de KPIs de calidad percibida (Limpieza, Amabilidad, Técnica) para asegurar estándares de excelencia en contratistas.

<img width="1202" height="493" alt="10" src="https://github.com/user-attachments/assets/9101e8cc-e9dd-4a0b-a239-aa85f6de507d" />

- **Gestión Basada en Datos**: Ranking de proveedores por tasa de cumplimiento de promesa y SLA de agendamiento, facilitando la toma de decisiones en la asignación de carga operativa.
