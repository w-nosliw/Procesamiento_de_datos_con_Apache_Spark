# Importamos librerías necesarias
from pyspark.sql import SparkSession, functions as F

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# Define la ruta del archivo CSV
file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'

# Lee el archivo CSV
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(file_path)

# Muestra el esquema del DataFrame
df.printSchema()

# Limpiar duplicados y eliminar valores nulos en columnas clave
df_clean = df.dropDuplicates().na.drop(subset=["EDAD", "SEXO", "TIPO DE VEHÍCULO", "COSTOS", "FECHA DE ACCIDENTE"])

# ---------------------- Análisis Exploratorio de Datos (EDA) ----------------------

# 1. Estadísticas básicas del conjunto de datos
print("Resumen de estadísticas del conjunto de datos:")
df_clean.summary().show()

# 2. Accidentes por tipo de vehículo
accidentes_por_vehiculo = df_clean.groupBy("TIPO DE VEHÍCULO").count().orderBy(F.col("count").desc())
print("Accidentes por tipo de vehículo:")
accidentes_por_vehiculo.show()

# 3. Distribución de accidentes por día de la semana
accidentes_por_dia = df_clean.groupBy("DIA SEMANA").count().orderBy(F.col("count").desc())
print("Distribución de accidentes por día de la semana:")
accidentes_por_dia.show()

# 4. Estadísticas de edad según el tipo de vehículo
edad_por_vehiculo = df_clean.groupBy("TIPO DE VEHÍCULO").agg(
    F.mean("EDAD").alias("Edad Promedio"),
    F.min("EDAD").alias("Edad Mínima"),
    F.max("EDAD").alias("Edad Máxima")
)
print("Estadísticas de edad por tipo de vehículo:")
edad_por_vehiculo.show()

# 5. Filtrado: Accidentes donde el usuario es conductor
accidentes_conductor = df_clean.filter(F.col("RELACION USUARIO/ACCIDENTE") == "CONDUCTOR")
print("Accidentes donde el usuario es conductor:")
accidentes_conductor.show(5)

# 6. Accidentes por grupo etario
accidentes_por_grupo_etario = df_clean.groupBy("GRUPO ETAREO").count().orderBy(F.col("count").desc())
print("Accidentes por grupo etario:")
accidentes_por_grupo_etario.show()

# 7. Promedio de tiempo de atención (en horas) en accidentes
promedio_atencion_horas = df_clean.groupBy("TIPO DE VEHÍCULO").agg(
    F.mean("OPORTUNIDAD DE LA ATENCIÓN EN HORAS").alias("Tiempo Promedio Atención (Horas)")
)
print("Promedio de tiempo de atención en horas según tipo de vehículo:")
promedio_atencion_horas.show()

# 8. Accidentes filtrados por altos costos de atención médica
accidentes_costos_altos = df_clean.filter(F.col("COSTOS") > 100000).select("NUMERO", "COSTOS", "TIPO DE VEHÍCULO", "SEXO")
print("Accidentes con costos mayores a 100,000:")
accidentes_costos_altos.show()

# 9. Número de accidentes por mes
accidentes_por_mes = df_clean.groupBy("MES").count().orderBy(F.col("count").desc())
print("Número de accidentes por mes:")
accidentes_por_mes.show()

# 10. Porcentaje de accidentes por sexo
accidentes_por_sexo = df_clean.groupBy("SEXO").count().withColumn("Porcentaje", (F.col("count") / df_clean.count()) * 100)
print("Porcentaje de accidentes por sexo:")
accidentes_por_sexo.show()

# Detener la sesión de Spark
spark.stop()
