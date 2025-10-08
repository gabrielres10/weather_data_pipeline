#!/bin/bash
# fix-permissions.sh
# Purpose: Ensure the current user and the Airflow service user share read/write/execute access
# to project directories (DAGs, logs, staging) by aligning ownership and group permissions.

echo "Setting shared permissions..."

PROJECT_DIR="$(dirname "$0")/.." 
AIRFLOW_GROUP="airflow"

# Create the group if it does not exist and add current user
sudo groupadd -f "$AIRFLOW_GROUP"
sudo usermod -aG "$AIRFLOW_GROUP" "$USER"

# Recursive ownership and group-writable (775) so Airflow containers (same GID) can write
sudo chown -R "$USER:$AIRFLOW_GROUP" "$PROJECT_DIR"
sudo chmod -R 775 "$PROJECT_DIR"

# Apply the setgid bit so new files inherit the group
sudo find "$PROJECT_DIR" -type d -exec chmod g+s {} \;

echo "Permissions configured:"
echo "Owner user: $USER | Group: $AIRFLOW_GROUP"
echo "Both the current user and Airflow processes can read/write DAGs and logs."
