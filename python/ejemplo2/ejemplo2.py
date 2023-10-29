 # Importar SparkSession
from pyspark.sql import SparkSession
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)

# Crear una SparkSession
spark = SparkSession.builder.appName("Ejemplo de Spark Core").getOrCreate()

# Leer un archivo CSV
df = spark.read.csv("data.csv", header=True)

# Contar el número de filas
num_rows = df.count()

# Imprimir el número de filas
print(num_rows)
