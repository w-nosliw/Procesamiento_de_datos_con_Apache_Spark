from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType

# Establecer nivel de logs en WARN
spark.sparkContext.setLogLevel("WARN")

# Definir el esquema actualizado para los datos de entrada
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", FloatType()),
    StructField("humidity", FloatType()),
    StructField("pressure", FloatType()),         # Nuevo campo de presión
    StructField("battery_level", FloatType()),    # Nuevo campo de nivel de batería
    StructField("wind_speed", FloatType()),       # Nuevo campo de velocidad del viento
    StructField("timestamp", TimestampType())
])

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("SensorDataAnalysis") \
    .getOrCreate()

# Configurar el lector de streaming para leer desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

# Parsear los datos JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Calcular estadísticas por ventana de tiempo (promedios por sensor y por minuto)
windowed_stats = parsed_df \
    .groupBy(window(col("timestamp"), "1 minute"), "sensor_id") \
    .agg({
        "temperature": "avg",
        "humidity": "avg",
        "pressure": "avg",
        "battery_level": "avg",
        "wind_speed": "avg"
    })

# Escribir los resultados en la consola
query = windowed_stats \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
