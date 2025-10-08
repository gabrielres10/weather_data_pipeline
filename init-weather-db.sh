#!/bin/bash
set -e

# Script de inicialización para crear base de datos del weather pipeline
# Se ejecuta automáticamente cuando postgres inicia por primera vez

echo "Creando base de datos weatherdb para el pipeline..."

# Crear base de datos weatherdb si no existe
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE weatherdb;
    GRANT ALL PRIVILEGES ON DATABASE weatherdb TO airflow;
EOSQL

echo "Base de datos weatherdb creada exitosamente"