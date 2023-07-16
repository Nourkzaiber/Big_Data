from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round

MIN_COUNT_RATINGS = 1000

# Erstellen Sie eine SparkSession
spark = SparkSession.builder.appName("StateRating").getOrCreate()

# Laden Sie die JSON-Daten in einen DataFrame
data = spark.read.json("projekt/yelp_academic_dataset_business.json")

# Filtern Sie den DataFrame, um nur die relevanten Spalten beizubehalten
filtered_data = data.select(col("state"), col("stars"))

# Gruppieren Sie nach "state" und berechnen Sie den Durchschnitt der "stars"
average_stars = filtered_data.groupBy("state").agg(
    round(avg("stars"), 2).alias("avg_stars"),
    count("stars").alias("count_stars")
)

# Filtern Sie nach Mindestanzahl an Bewertungen
filtered_average_stars = average_stars.filter(col("count_stars") >= MIN_COUNT_RATINGS)

# Sortieren Sie nach absteigendem Durchschnitt der Sternebewertungen
sorted_average_stars = filtered_average_stars.orderBy(col("avg_stars").desc())

# Zeigen Sie die Ergebnisse an
sorted_average_stars.show()

# Stoppen Sie die SparkSession
spark.stop()