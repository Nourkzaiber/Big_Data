# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import col

# Spark Session erstellen
spark = SparkSession.builder.appName("Prognose").getOrCreate()

# Schema für die Daten definieren
schema = StructType([
    StructField("years", IntegerType(), nullable=True),
    StructField("user_counts", IntegerType(), nullable=True)
])

# Daten aus der CSV-Datei lesen
data = spark.read.csv("user_count_by_year.csv", schema=schema)

# Prognose für die nächsten 5 Jahre
prediction_years = range(2023, 2028)

# Daten für die Prognose vorbereiten
data = data.withColumn("years", data["years"].cast(IntegerType()))

# Feature-Vektor erstellen
assembler = VectorAssembler(inputCols=["years"], outputCol="features")
data = assembler.transform(data)

# Lineare Regression anwenden
lr = LinearRegression(featuresCol="features", labelCol="user_counts")
model = lr.fit(data)

# Daten für die Prognose generieren
prediction_data = spark.createDataFrame([(year,) for year in prediction_years], ["years"])

# Feature-Vektor für die Prognose erstellen
prediction_data = assembler.transform(prediction_data)

# Prognose durchführen
predictions = model.transform(prediction_data)

# Ergebnisse anzeigen
results = predictions.select("years", predictions["prediction"].cast(IntegerType()).alias("prediction"))

# Ergebnisse für die nächsten Jahre anzeigen
results.show()


spark.stop()