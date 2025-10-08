# Design Decisions - Weather Data Pipeline (Phase 3)

## Objetivo
Documentar las decisiones técnicas de la fase 3: diseño de esquema PostgreSQL y estrategia de carga/upsert.

## Esquema
Tablas principales:
- `weather_raw`: Datos granulares por ciudad y timestamp de medición.
- `daily_weather_summary`: Agregados diarios con métricas de temperatura y extremos.

### Tabla `weather_raw`
Campos clave:
- `city_name`, `measurement_time` definen la unicidad lógica de una fila de observación.
- `unique_run_key` opcional para idempotencia basada en la ejecución/fuente.

Índices:
- `idx_weather_raw_city` para filtros analíticos por ciudad.
- `idx_weather_raw_measurement_time` para rangos temporales.

Constraints:
- `uq_weather_city_time` asegura que cada ciudad-timestamp tenga una sola fila (permite upsert).
- `uq_weather_raw_run` asegura que un mismo identificador de ejecución no duplique registros.

### Tabla `daily_weather_summary`
- Llave primaria surrogate (`id`) + constraint UNIQUE en `date`.
- Facilita upsert por `date` (ON CONFLICT (date) ...).

## Estrategia de Upsert
### weather_raw
Se usa:
```sql
ON CONFLICT (city_name, measurement_time) DO UPDATE
```
para refrescar métricas (temperatura, presión, humedad) si se reprocesa el mismo timestamp.

Se rellena `unique_run_key` como `<city>_<measurement_time>` para trazabilidad; puede aprovecharse en auditoría.

### daily_weather_summary
Upsert diario simple:
```sql
ON CONFLICT (date) DO UPDATE
```
Actualiza métricas agregadas y refresca `created_at` a CURRENT_TIMESTAMP.

## Decisiones Clave
| Área | Decisión | Justificación |
|------|----------|---------------|
| Idempotencia raw | conflict (city, measurement_time) | Representa snapshot único (observación instantánea) |
| Clave adicional | unique_run_key | Flexibilidad para futuros flujos con run_id / archivo fuente |
| Resumen diario | 1 row/day | Acceso rápido a KPIs y evita recalcular sobre raw |
| Formato ingestión | Parquet particionado por date | Mejora lectura selectiva futura en Spark / analítica |
| Batch size upsert | 500 | Balance entre round-trips y memoria | 
| Librería DB | psycopg v3 binary | Mejor performance y soporte moderno |
| Conversión tipos | pandas + pyarrow | Coherente con stack existente y eficiente |

## Flujo de Carga (`load_to_postgres.py`)
1. Descubre archivos parquet bajo `staging/processed/weather_parquet/date=*/`.
2. Concatena DataFrames en memoria (dataset pequeño controlado).
3. Genera `unique_run_key` y ejecuta upserts en lotes.
4. Procesa summary más reciente (`summary_*.json`).
5. Confirma transacción al final.

## Riesgos y Mitigaciones
| Riesgo | Mitigación |
|--------|-----------|
| Aumento de volumen | Cambiar a COPY + staging temp table si supera ~100K filas por batch |
| Timestamps duplicados por reprocesos divergentes | Upsert garantiza consistencia y evita duplicados |
| Falta de columnas futuras | Permitir schema evolution en Parquet y ALTER TABLE en DB |
| Fallos parciales (raw ok, summary falla) | Transacción por conexión; se aborta si summary lanza excepción |

## Extensiones Futuras
- Añadir tabla `weather_city_latest` materializada (o view) para consultas rápidas.
- Integrar Airflow DAG con tasks: extract -> process -> load.
- Añadir auditoría (`loaded_at`, `source_file`).
- Implementar validaciones de integridad (ej. rango humedad 0–100) antes del upsert.

## Consideraciones de Performance
Para el tamaño actual (pocas ciudades y frecuencia moderada) la solución es suficiente. Si se escala:
- Migrar a COPY o `psycopg.copy()` desde buffer parquet->CSV.
- Usar particionado temporal en `weather_raw` (HASH por año/mes).
- Agregar índice compuesto (city_name, measurement_time DESC) para consultas últimas observaciones.

## Notas Finales
El diseño busca equilibrio entre simplicidad, trazabilidad y capacidad de evolucionar sin refactorizaciones drásticas.
