from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, desc, count

spark = SparkSession.builder \
    .appName("AnalyseNoteFilm") \
    .getOrCreate()

# Lecture depuis HDFS
Data_Data_Raw = spark.read.text("hdfs://namenode:9000/mr/donne/u.data")

# Découpage simple des lignes
parts = split(col("value"), "\t")

Data_Data = Data_Data_Raw.select(
    parts.getItem(0).cast("int").alias("user id"),
    parts.getItem(1).cast("int").alias("movie id"),
    parts.getItem(2).cast("int").alias("rating")
)

Item_Data_Raw = spark.read.text("hdfs://namenode:9000/mr/donne/u.item")
Genre_Data_Raw = spark.read.text("hdfs://namenode:9000/mr/donne/u.genre")

parts = split(col("value"), "\|")

Item_Data = Item_Data_Raw.select(
    parts.getItem(0).cast("int").alias("movie id"),
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

Genre_Data = Genre_Data_Raw.select(
    parts.getItem(0).alias("genre"),
    parts.getItem(1).cast("int").alias("genre_id")
)


# genres = [row["genre"] for row in Genre_Data.collect()]
# backtick_genres = ["Children's", "Film-Noir", "Sci-Fi"]
# stack_expr = '"""stack(' + str(len(genres)) + ",\n"
# for g in genres:
#     if g in backtick_genres:
#         stack_expr += f"    '{g}', `{g}`,\n"
#     else:
#         stack_expr += f"    '{g}', {g},\n"
# stack_expr = stack_expr.rstrip(",\n") + "\n"
# stack_expr += "    ) as (genre, value)\"\"\""
stack_expr =  (
    """stack(19,
    'unknown', unknown,
    'Action', Action,
    'Adventure', Adventure,
    'Animation', Animation,
    'Children', `Children's`,
    'Comedy', Comedy,
    'Crime', Crime,
    'Documentary', Documentary,
    'Drama', Drama,
    'Fantasy', Fantasy,
    'Film-Noir', `Film-Noir`,
    'Horror', Horror,
    'Musical', Musical,
    'Mystery', Mystery,
    'Romance', Romance,
    'Sci-Fi', `Sci-Fi`,
    'Thriller', Thriller,
    'War', War,
    'Western', Western
    ) as (genre, value)"""
)

joinTable = Item_Data.join(Data_Data, "movie id")

print("\n==============================")
print("Les 10 films ayant la meilleure note moyenne (avec leur titre)")
print("==============================")
joinTable.groupBy("movie title").avg("rating").withColumnRenamed("avg(rating)", "rating moyen").orderBy(desc("rating moyen")).show(10)


print("\n==============================")
print("Les 10 films ayant la plus mauvaise note moyenne (avec leur titre)")
print("==============================")
joinTable.select(["movie title","rating"]).avg("rating").withColumnRenamed("avg(rating)", "rating moyen").orderBy("rating").show(10)


print("\n==============================")
print("Le genre recevant le plus de « bonnes notes » (une bonne note est définie par rating >= 4)")
print("==============================")
joinTable.filter(Data_Data.rating >= 4).selectExpr("rating",stack_expr).filter(col("value") == 1).groupBy("genre").count().withColumnRenamed("count", "nb_film").orderBy(desc("nb_film")).show(1)

print("\n==============================")
print("Le genre recevant le moins de « bonnes notes » (une mauvaise note est définie par rating <= 2)")
print("==============================")
joinTable.filter(Data_Data.rating <= 2).selectExpr("rating",stack_expr).filter(col("value") == 1).groupBy("genre").count().withColumnRenamed("count", "nb_film").orderBy(desc("nb_film")).show(1)

print("\n==============================")
print("Une statistique pertinente par utilisateur (nombre de films notés)")
print("==============================")
joinTable.groupBy("user id").count().withColumnRenamed("count", "nb_films_notes").orderBy(desc("nb_films_notes")).show(truncate=False)


print("\n==============================")
print("Une statistique pertinente par utilisateur (moyenne des notes)")
print("==============================")


# NB film moyen
count = joinTable.groupBy("user id").count().withColumnRenamed("count", "nb_films_notes")
# Avg des notes
avg = joinTable.groupBy("user id").avg("rating").withColumnRenamed("avg(rating)", "note moyenne")
# fusion
requete = count.join(avg, "user id")
requete.orderBy(desc("note moyenne")).show(truncate=False)

spark.stop()