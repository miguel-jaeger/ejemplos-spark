# Importa la biblioteca de Spark
from pyspark import SparkContext

# Crea una sesión Spark
sc = SparkContext("local", "Flujo de Datos en Spark")

# Crea un RDD con datos de muestra (números del 1 al 10)
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd = sc.parallelize(data)

# Transformación 1: Filtrar los números pares
rdd_pares = rdd.filter(lambda x: x % 2 == 0)

# Transformación 2: Elevar al cuadrado los números
rdd_cuadrados = rdd_pares.map(lambda x: x**2)

# Transformación 3: Filtrar números mayores que 10
rdd_mayores_10 = rdd_cuadrados.filter(lambda x: x > 10)

# Transformación 4: Sumar todos los números
resultado = rdd_mayores_10.reduce(lambda x, y: x + y)

# Imprimir el resultado
print("Resultado final:", resultado)

# Detén la sesión Spark
sc.stop()