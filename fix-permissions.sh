#!/bin/bash
# fix-permissions.sh
# Script para corregir permisos después de levantar Docker Compose

echo "Configurando permisos para Airflow..."

# Permisos para staging (datos del pipeline)
sudo chown -R 50000:0 staging/
sudo chmod -R 755 staging/

# Permisos para logs de Airflow
sudo chown -R 50000:0 logs/
sudo chmod -R 755 logs/

# Mantener permisos de edición para el usuario local en dags
sudo chown -R $USER:$USER dags/
sudo chmod -R 755 dags/

echo "✅ Permisos configurados correctamente"
echo "🎯 staging/ y logs/ → Airflow (50000:0)"  
echo "📝 dags/ → Usuario local ($USER)"
echo ""
echo "Ahora puedes ejecutar el DAG sin problemas de permisos"