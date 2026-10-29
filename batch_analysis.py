from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, lower

def main():
    # Inicializar la sesión de Spark para Batch
    spark = SparkSession.builder \
        .appName("BatchAnalysis_CalidadDatos_Avanzado") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    file_path = "Listado_de_Errores_Frecuentes_de_Calidad_de_Datos_Abiertos_20261016.csv"

    print("==================================================")
    print("--- 1. CARGA DEL DATASET DE CALIDAD DE DATOS ---")
    print("==================================================")
    df = spark.read.option("header", "true") \
                   .option("inferSchema", "true") \
                   .option("multiline", "true") \
                   .csv(file_path)

    df.printSchema()
    total_registros = df.count()
    print(f"\n[INFO] Total de errores catalogados: {total_registros}")

    print("\n==================================================")
    print("--- 2. LIMPIEZA Y TRANSFORMACIÓN DE DATOS ---")
    print("==================================================")
    # Limpieza: eliminar filas con código de error nulo y agregar estado de auditoría
    df_clean = df.na.drop(subset=["Código de Error", "Categoría de Error"])
    df_transformed = df_clean.withColumn("Estado_Auditoria", lit("Verificado_PySpark"))

    print("\n==================================================")
    print("--- 3. ANÁLISIS EXPLORATORIO (EDA) Y CONSULTAS ---")
    print("==================================================")

    print("\n>>> CONSULTA 1: Conteo de Errores por Categoría Principal")
    df_transformed.groupBy("Categoría de Error") \
                  .count() \
                  .orderBy(col("count").desc()) \
                  .show(truncate=False)

    print("\n>>> CONSULTA 2: Filtrado de Errores de Metadatos")
    df_transformed.filter(col("Categoría de Error") == "Metadata Errada, Incompleta y/o Vacía") \
                  .select("Código de Error", "Tipo de Error", "Solución") \
                  .show(5, truncate=False)

    print("\n>>> CONSULTA 3: Errores que cuentan con Video Tutorial de Apoyo")
    df_transformed.filter(lower(col("Herramientas de Apoyo")).contains("youtube")) \
                  .select("Código de Error", "Tipo de Error", "Herramientas de Apoyo") \
                  .show(5, truncate=False)

    print("\n>>> CONSULTA 4: Ejecución mediante Spark SQL Nativo")
    # Crear Vista Temporal para consultas SQL
    df_transformed.createOrReplaceTempView("vista_errores_calidad")

    consulta_sql = spark.sql("""
        SELECT `Código de Error`, `Categoría de Error`, `Tipo de Error`
        FROM vista_errores_calidad
        WHERE `Tipo de Error` LIKE '%columna%' OR `Tipo de Error` LIKE '%filas%'
    """)
    consulta_sql.show(10, truncate=False)

    print("\n==================================================")
    print("[SUCCESS] Proceso Batch completado exitosamente.")
    print("==================================================")

    spark.stop()

if __name__ == "__main__":
    main()
