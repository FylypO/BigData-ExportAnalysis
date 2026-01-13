from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, first, lag, round
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("PrepareWDIData") \
    .master("local[*]") \
    .getOrCreate()

input_path = "hdfs://localhost:8020/user/vagrant/project/WDI/*.parquet"

# Load data and map types
df = spark.read.parquet(input_path) \
    .withColumn("value", col("value").cast("double")) \
    .withColumn("year", col("year").cast("integer"))

# Pivot table
pivoted_df = df.groupBy("country_id", "country_name", "year") \
    .pivot("indicator_name") \
    .agg(first("value"))

# Renaming columns
rename_map = {
    "External debt stocks, total (DOD, current US$)": "external_debt",
    "GDP (current US$)": "gdp",
    "Imports of goods and services (current US$)": "import",
    "Industry (including construction), value added (% of GDP)": "industry_in_gdp",
    "Inflation, consumer prices (annual %)": "inflation",
    "Population, total": "population",
    "Services, value added (% of GDP)": "services_in_gdp",
    "Trade (% of GDP)": "trade_in_gdp"
}

for old_name, new_name in rename_map.items():
    pivoted_df = pivoted_df.withColumnRenamed(old_name, new_name)

# Calculate new indicators
# window_spec = Window.partitionBy("country_id").orderBy("year")

final_df = pivoted_df \
    .withColumn("gdp_per_capita", round(col("gdp") / col("population"), 2)) \
        .drop("indicator_name")
    # .withColumn("prev_year_gdp", lag("gdp").over(window_spec)) \
    # .withColumn("gdp_growth", round(((col("gdp") - col("prev_year_gdp")) / col("prev_year_gdp")) * 100, 2)) \
        # .drop("prev_year_gdp") \



final_df = final_df.orderBy("country_name", "year")
final_df.printSchema()
print("DataFrame count:", final_df.count())
poland_df = final_df.filter(col("country_name") == "Poland")
poland_df.show(15)