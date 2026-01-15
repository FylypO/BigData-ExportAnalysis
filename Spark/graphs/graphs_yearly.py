from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from plotly.subplots import make_subplots
import plotly.graph_objects as go

spark = SparkSession.builder \
    .appName("Top6ImporterPlots") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("hdfs:///user/vagrant/project/final_tables/comtrade_with_wdi.parquet")

######################################################################################
#IMPORT, GDP
countries = [
    "Russian Federation",
    "Germany",
    "Nigeria",
    "Brazil",
    "United States",
    "Australia"
]

df_f = df.filter(col("country_name").isin(countries))

data = {}
for c in countries:
    rows = df_f.filter(col("country_name") == c).orderBy("year").collect()
    data[c] = {
        "year": [float(r["year"]) for r in rows],
        "gdp": [float(r["gdp_per_capita"]) for r in rows],
        "import": [float(r["annual_value_usd"]) for r in rows]
    }


fig = make_subplots(
    rows=2, cols=3,
    subplot_titles=countries,
    specs=[[{"secondary_y": True}]*3,
           [{"secondary_y": True}]*3],
    horizontal_spacing=0.12,
    vertical_spacing=0.18
)

pos = {
    0:(1,1), 1:(1,2), 2:(1,3),
    3:(2,1), 4:(2,2), 5:(2,3)
}

for i, c in enumerate(countries):
    r, cl = pos[i]

    fig.add_scattergl(
        x=data[c]["year"],
        y=data[c]["gdp"],
        name="GDP per capita",
        mode="lines",
        showlegend=(i == 0),
        row=r, col=cl, secondary_y=False
    )

    fig.add_scattergl(
        x=data[c]["year"],
        y=data[c]["import"],
        name="Import USD",
        mode="lines",
        showlegend=(i == 0),
        row=r, col=cl, secondary_y=True
    )


fig.update_layout(
    width=1850,
    height=1050,
    title="GDP per Capita and Import of Selected Countries",
    legend=dict(
        orientation="h",
        x=0.5,
        y=-0.12,
        xanchor="center",
        font=dict(size=16)
    ),
    margin=dict(l=80, r=80, t=100, b=140)
)

for i in range(6):
    r, cl = pos[i]
    fig.update_yaxes(title_text="GDP per Capita", row=r, col=cl, secondary_y=False)
    fig.update_yaxes(title_text="Import USD", row=r, col=cl, secondary_y=True)

fig.show()

spark.stop()
