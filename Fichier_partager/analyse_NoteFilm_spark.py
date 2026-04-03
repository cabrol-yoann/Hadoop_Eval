from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, desc

spark = SparkSession.builder \
    .appName("AnalyseNoteFilm") \
    .getOrCreate()

# Lecture depuis HDFS
Data_Data_Raw = spark.read.text("hdfs://namenode:9000/mr/donne/u.data")

# Découpage simple des lignes
parts = split(col("value"), "\t")

Data_Data = Data_Data_Raw.select(
    parts.getItem(0).alias("user id"),
    parts.getItem(1).alias("item id"),
    parts.getItem(2).alias("rating")
)

Item_Data_Raw = spark.read.text("hdfs://namenode:9000/mr/donne/u.item")
# User_Data_Raw = spark.read.text("hdfs://namenode:9000/mr/donne/u.user")

parts = split(col("value"), "\|")

Item_Data = Item_Data_Raw.select(
    parts.getItem(0).alias("movie id"),
    parts.getItem(1).alias("movie title"),
    parts.getItem(5).alias("unknown"),
    parts.getItem(6).alias("Action"),
    parts.getItem(7).alias("Adventure"),
    parts.getItem(8).alias("Animation"),
    parts.getItem(9).alias("Children's"),
    parts.getItem(10).alias("Comedy"),
    parts.getItem(11).alias("Crime"),
    parts.getItem(12).alias("Documentary"),
    parts.getItem(13).alias("Drama"),
    parts.getItem(14).alias("Fantasy"),
    parts.getItem(15).alias("Film-Noir"),
    parts.getItem(16).alias("Horror"),
    parts.getItem(17).alias("Musical"),
    parts.getItem(18).alias("Mystery"),
    parts.getItem(19).alias("Romance"),
    parts.getItem(20).alias("Sci-Fi"),
    parts.getItem(21).alias("Thriller"),
    parts.getItem(22).alias("War"),
    parts.getItem(23).alias("Western")
)

# User_Data = User_Data_Raw.select(
#     parts.getItem(0).alias("movie id"),
#     parts.getItem(1).alias("movie title"),
#     parts.getItem(5).alias("unknown"),
#     parts.getItem(5).alias("Action")
# )

print("\n==============================")
print("value")
print("==============================")

Data_Data.show(truncate=False)
Item_Data.show(truncate=False)

spark.stop()