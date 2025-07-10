#!/bin/bash

# Script para descargar fixtures de Polygon.io S3
# Ejecutar desde el directorio raíz del proyecto

# Crear directorio fixtures
mkdir -p test/fixtures/raw

# Array de fechas para descargar (enero 2024)
dates=(
   "2024-01-02"
   "2024-01-03" 
   "2024-01-04"
   "2024-01-05"
   "2024-01-08"
   "2024-01-09"
   "2024-01-10"
   "2024-01-11"
   "2024-01-12"
)

echo "Downloading Polygon.io S3 fixtures..."
echo "Target directory: test/fixtures/raw/"
echo ""

# Contador para progreso
total=${#dates[@]}
current=0

# Bucle para descargar cada archivo
for date in "${dates[@]}"; do
   current=$((current + 1))
   filename="${date}.csv.gz"
   s3_path="s3://flatfiles/us_stocks_sip/day_aggs_v1/2024/01/${filename}"
   local_path="test/fixtures/raw/${filename}"
   
   echo "[$current/$total] Downloading $filename..."
   
   if aws s3 cp "$s3_path" "$local_path" \
       --profile polygon \
       --endpoint-url https://files.polygon.io; then
       
       # Mostrar tamaño del archivo descargado
       size=$(du -h "$local_path" | cut -f1)
       echo "  ✅ Downloaded: $filename ($size)"
   else
       echo "  ❌ Failed to download: $filename"
   fi
   
   echo ""
done

echo "Download completed!"
echo ""
echo "Downloaded files:"
ls -lh test/fixtures/raw/

echo ""
echo "Next steps:"
echo "1. Run: python test/create_fixtures.py (to create test-sized versions)"
echo "2. Add test/fixtures/raw/ to .gitignore (large files)"
echo "3. Commit test/fixtures/*.csv.gz (small test files)"