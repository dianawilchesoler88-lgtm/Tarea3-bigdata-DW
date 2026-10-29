# Tarea 3: Procesamiento en Batch y Tiempo Real con Apache Spark - Big Data (UNAD)

Repositorio que contiene las soluciones prácticas desarrolladas en la máquina virtual Ubuntu Server utilizando PySpark, Kafka y Spark SQL.

## 📂 Contenido del Repositorio
- `batch_analysis.py`: Script en PySpark para el procesamiento por lotes (Batch), limpieza, EDA y consultas SQL sobre el dataset de calidad de datos abiertos.

## ⚙️ Instrucciones de Ejecución (Batch)
1. Asegurarse de tener el archivo CSV en el directorio de trabajo:
   `Listado_de_Errores_Frecuentes_de_Calidad_de_Datos_Abiertos_20261016.csv`
2. Ejecutar el script en la terminal mediante Spark:
   ```bash
   spark-submit batch_analysis.py
