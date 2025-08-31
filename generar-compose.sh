#!/bin/bash

echo "Generando archivo $1 con $2 clientes"
python3 generate-docker.py $1 $2

echo "Archivo generado con exito"