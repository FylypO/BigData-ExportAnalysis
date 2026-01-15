from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, concat_ws, lit, split, sum as spark_sum
import numpy as np
import builtins
import plotly.graph_objects as go

spark = SparkSession.builder \
    .appName("ReadCurrencyParquet") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("hdfs:///user/vagrant/project/final_tables/comtrade_with_currency.parquet")
df.show(10)

######################################################################################
#DEU, EUR
df_de = df.filter((col("partnerISO") == "DEU") & (col("currency") == "EUR")) \
          .orderBy("year", "month")

rows = df_de.collect()
months = [f"{r['year']}-{r['month']:02d}" for r in rows]
exports = [r['primary_value_usd'] for r in rows]
rates = [r['avg_rate'] for r in rows]

fig = go.Figure()
fig.add_bar(x=months, y=exports, name="Export to Germany (USD)", yaxis="y1")
fig.add_scatter(x=months, y=rates, name="PLN/EUR", yaxis="y2", mode="lines+markers")

fig.update_layout(
    yaxis=dict(title="Export (USD)"),
    yaxis2=dict(title="PLN/EUR", overlaying="y", side="right")
)
fig.show()

######################################################################################
#COMODITIES
df_f = df.filter(col("partnerISO") != "W00") \
         .withColumn("date", to_date(concat_ws("-", col("year"), col("month"), lit(1)))) \
         .withColumn("commodity", split(col("commodity_desc"), ";").getItem(0))

df_g = df_f.groupBy("commodity", "date") \
           .agg(spark_sum("primary_value_usd").alias("value"))

dates = sorted({r["date"] for r in df_g.select("date").collect()})
data = {}

for r in df_g.collect():
    data.setdefault(r["commodity"], [0]*len(dates))
    data[r["commodity"]][dates.index(r["date"])] = r["value"]

fig = go.Figure()
for k, v in data.items():
    fig.add_scatter(x=dates, y=v, stackgroup="one", mode="lines", name=k)

fig.show()

######################################################################################
#MAP
df_agg = df.filter(col("year") == 2023) \
           .groupBy("partnerISO") \
           .agg(spark_sum("primary_value_usd").alias("export"))

rows = [r for r in df_agg.collect() if r["partnerISO"] != "W00"]
countries = [r["partnerISO"] for r in rows]
exports = [r["export"] for r in rows]

top = builtins.max(exports)
top_country = countries[exports.index(top)]

z = np.log10(np.array(exports) + 1)

fig = go.Figure(go.Choropleth(
    locations=countries,
    z=z,
    customdata=exports,
    hovertemplate="%{location}<br>$%{customdata:,.0f}<extra></extra>"
))

fig.update_layout(
    title=f"Total exports 2023 – top: {top_country} (${top:,.0f})"
)
fig.show()

spark.stop()

######################################################################################
# EURO I NIEMCY

df_de = df.filter((col("partnerISO") == "DEU") & (col("currency") == "EUR"))
df_de = df_de.orderBy("year", "month")

data_collect = df_de.collect()
months = [f"{row['year']}-{row['month']:02d}" for row in data_collect]
exports = [row['primary_value_usd'] for row in data_collect]
euro_rates = [row['avg_rate'] for row in data_collect]

fig = go.Figure()
fig.add_trace(go.Bar(
    x=months,
    y=exports,
    name="Export to Germany (USD)",
    yaxis="y1",
    marker_color="blue"
))
fig.add_trace(go.Scatter(
    x=months,
    y=euro_rates,
    name="average PLN/EUR exchange rate",
    yaxis="y2",
    mode="lines+markers",
    marker_color="red"
))
fig.update_layout(
    title="Export to Germany vs PLN/EUR exchange rate",
    xaxis=dict(title="Month"),
    yaxis=dict(title="Export to Germany (USD)", side="left"),
    yaxis2=dict(title="average PLN/EUR exchange rate", overlaying="y", side="right"),
    legend=dict(x=0.1, y=1.1, orientation="h")
)
fig.show()

##########################################################################################
# Z PODZIAŁEM NA TOWARY

df_filtered = df.filter(col('partnerISO') != 'W00')
df_filtered = df_filtered.withColumn('date', to_date(concat_ws('-', col('year'), col('month'), lit(1))))
df_filtered = df_filtered.withColumn('commodity_short', split(col('commodity_desc'), ';').getItem(0))

df_grouped = df_filtered.groupBy('commodity_short', 'date') \
                        .agg(spark_sum('primary_value_usd').alias('commodity_value'))
df_total = df_filtered.groupBy('date').agg(spark_sum('primary_value_usd').alias('total_export'))

grouped_rows = df_grouped.collect()
total_rows = {row['date']: row['total_export'] for row in df_total.collect()}

plot_data = {}
dates = sorted(total_rows.keys())

for row in grouped_rows:
    commodity = row['commodity_short']
    date = row['date']
    value = row['commodity_value']
    if commodity not in plot_data:
        plot_data[commodity] = [0]*len(dates)
    idx = dates.index(date)
    plot_data[commodity][idx] = value

fig = go.Figure()
for commodity, values in plot_data.items():
    fig.add_trace(go.Scatter(
        x=dates,
        y=values,
        mode='lines',
        stackgroup='one',
        name=commodity,
        line=dict(width=0.5),
        opacity=0.7
    ))

fig.update_layout(
    title="Export by commodity",
    xaxis_title="Date",
    yaxis_title="Value of exports (USD)",
    template='plotly_white'
)
fig.show()

############################################################################
# MAPA

df_2025 = df.filter(col("year") == 2023)
df_agg = df_2025.groupBy("partnerISO").agg(spark_sum("primary_value_usd").alias("total_export_usd"))

rows = df_agg.collect()
countries = [row['partnerISO'] for row in rows if row['partnerISO'] != "W00"]
exports = [row['total_export_usd'] for row in rows if row['partnerISO'] != "W00"]

max_index = exports.index(builtins.max(exports))
top_country = countries[max_index]
top_export_value = exports[max_index]

exports_log = np.log10(np.array(exports) + 1)
n_ticks = 6
log_min, log_max = exports_log.min(), exports_log.max()
tickvals = np.linspace(log_min, log_max, n_ticks)
ticktext = [f"${int(10**v - 1):,}" for v in tickvals]

two_color_scale = [
    [0, "#e0ecf4"],
    [0.75, "#9ebcda"],
    [1, "#8856a7"]
]

fig = go.Figure(data=go.Choropleth(
    locations=countries,
    z=exports_log,
    colorscale=two_color_scale,
    colorbar=dict(title="Total Export USD", tickvals=tickvals, ticktext=ticktext),
    customdata=np.array(exports),
    hovertemplate='<b>%{location}</b><br>Export: $%{customdata:,.0f}<extra></extra>'
))
fig.update_layout(
    title_text=f'Total Exports by Country in 2023 (Top: {top_country} - ${top_export_value:,.0f})',
    geo=dict(showframe=False, showcoastlines=True)
)
fig.show()

spark.stop()
