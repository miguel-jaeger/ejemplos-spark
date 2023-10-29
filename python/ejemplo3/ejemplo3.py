from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator

# Crear una sesión Spark
spark = SparkSession.builder.getOrCreate()

# Leer tus datos desde un archivo CSV directamente en un DataFrame de Spark
data = spark.read.csv("personal_data.csv", header=True, inferSchema=True)

# Dividir los datos en conjuntos de entrenamiento y prueba (80% para entrenamiento y 20% para prueba)
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# Convertir la columna "x" en un vector de características
assembler = VectorAssembler(inputCols=["x"], outputCol="features")
train_data = assembler.transform(train_data)
test_data = assembler.transform(test_data)

# Renombrar la columna "y" a "label"
train_data = train_data.withColumnRenamed("y", "label")
test_data = test_data.withColumnRenamed("y", "label")

# Entrenar el modelo de regresión lineal
lr = LinearRegression().fit(train_data)

# Evaluar el modelo en los datos de prueba
test_predictions = lr.transform(test_data)
evaluator = RegressionEvaluator(metricName="rmse")
rmse = evaluator.evaluate(test_predictions)

# Imprimir la precisión del modelo en el conjunto de prueba
print("Error cuadrático medio de la raíz (RMSE) en el conjunto de prueba:", rmse)
