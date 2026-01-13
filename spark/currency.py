from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, to_date
from pyspark.sql.functions import min, max, avg

spark = SparkSession.builder \
    .appName("ReadCurrencyParquet") \
    .getOrCreate()
#ADJUST PATH
df = spark.read.parquet("hdfs:///user/vagrant/data/currency_all.parquet")

df = df.withColumn(
    "date", to_date(col("date"), "yyyy-MM-dd")
).withColumn(
    "year", year(col("date"))
).withColumn(
    "month", month(col("date"))
)
yearly_agg = (
    df.groupBy("currency", "year")
      .agg(
          min("rate").alias("min_rate"),
          max("rate").alias("max_rate"),
          avg("rate").alias("avg_rate")
      )
      .orderBy("currency", "year")
)

monthly_agg = (
    df.groupBy("currency","year", "month")
      .agg(
          min("rate").alias("min_rate"),
          max("rate").alias("max_rate"),
          avg("rate").alias("avg_rate")
      )
      .orderBy("currency","year", "month")
)


monthly_agg.show(10)
monthly_agg.printSchema()

spark.stop()
