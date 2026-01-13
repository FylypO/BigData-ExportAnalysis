from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("DimCurrencyCorrect").getOrCreate()

data = [
    Row(country_iso3="USA", currency="USD"),
    Row(country_iso3="GBR", currency="GBP"),
    Row(country_iso3="NOR", currency="NOK"),
    Row(country_iso3="SWE", currency="SEK"),
    Row(country_iso3="DNK", currency="DKK"),
    Row(country_iso3="DEU", currency="EUR"),
    Row(country_iso3="FRA", currency="EUR"),
    Row(country_iso3="ESP", currency="EUR"),
    Row(country_iso3="ITA", currency="EUR"),
    Row(country_iso3="CZE", currency="CZK"),
    Row(country_iso3="HUN", currency="HUF"),
    Row(country_iso3="NZL", currency="NZD"),
    Row(country_iso3="AUS", currency="AUD"),
    Row(country_iso3="CAN", currency="CAD"),
    Row(country_iso3="JPN", currency="JPY"),
    Row(country_iso3="CHN", currency="CNY"),
    Row(country_iso3="THA", currency="THB"),
    Row(country_iso3="SGP", currency="SGD"),
    Row(country_iso3="ZAF", currency="ZAR"),
    Row(country_iso3="BRA", currency="BRL"),
    Row(country_iso3="IND", currency="INR"),
    Row(country_iso3="PHL", currency="PHP"),
    Row(country_iso3="MYS", currency="MYR"),
    Row(country_iso3="IDN", currency="IDR"),
    Row(country_iso3="KOR", currency="KRW"),
    Row(country_iso3="ROU", currency="RON"),
    Row(country_iso3="IRL", currency="EUR"),
    Row(country_iso3="BEL", currency="EUR"),
    Row(country_iso3="NLD", currency="EUR"),
    Row(country_iso3="EST", currency="EUR"),
    Row(country_iso3="AUT", currency="EUR"),
    Row(country_iso3="SVN", currency="EUR"),
    Row(country_iso3="SVK", currency="EUR"),
    Row(country_iso3="LVA", currency="EUR"),
    Row(country_iso3="LTU", currency="EUR"),
    Row(country_iso3="CYP", currency="EUR"),
    Row(country_iso3="BGR", currency="BGN"),

    Row(country_iso3="X1", currency=None),
    Row(country_iso3="_X", currency=None),
    Row(country_iso3="W00", currency=None),
    Row(country_iso3="XX", currency=None),
    Row(country_iso3="E19", currency=None),
    Row(country_iso3="S19", currency=None)
]

df_dim_currency = spark.createDataFrame(data)

df_dim_currency.show(truncate=False)
#ADJUST PATH
df_dim_currency.write.mode("overwrite").parquet("hdfs:///user/vagrant/dim/dim_currency_country.parquet")

spark.stop()
