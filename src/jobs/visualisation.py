import plotly.express as px
from pyspark.sql import SparkSession

# 1. Initialize Spark
spark = SparkSession.builder.appName('Japan Visa Viz').getOrCreate()

# 2. READ the Cleaned Data (From File 1)
# Note: We read the output of the previous script
df = spark.read.csv('/opt/spark/output/visa_number_in_japan_cleaned.csv', header=True, inferSchema=True)

# 3. Create Temp View for SQL
# This allows your existing SQL queries to work exactly as before
df.createOrReplaceTempView('japan_visa')

# --- VISUALISATION 1: Visa by Continent ---
df_cont = spark.sql("""
    SELECT year, continent, sum(number_of_issued_numerical) visa_issued
    FROM japan_visa
    WHERE continent IS NOT NULL
    GROUP BY year, continent
""")

df_cont = df_cont.toPandas()

fig = px.bar(df_cont, x='year', y='visa_issued', color='continent', barmode='group')
fig.update_layout(
    title_text="Number of visa issued in Japan between 2006 and 2017",
    xaxis_title='Year',
    yaxis_title='Number of visa issued',
    legend=dict(
        title='Continent'
    ),
    margin=dict(t=80, r=200, b=80, l=60)
)
fig.write_html('/opt/spark/output/visa_number_in_japan_continent_2006_2017.html')


# --- VISUALISATION 2: Top 10 Countries 2017 ---
df_country = spark.sql("""
    SELECT country, sum(number_of_issued_numerical) visa_issued
    FROM japan_visa
    WHERE country NOT IN ('total', 'others')
    AND country IS NOT NULL
    AND year = 2017
    GROUP BY country
    ORDER BY visa_issued DESC
    LIMIT 10
""")

df_country = df_country.toPandas()

fig = px.bar(df_country, x='country', y='visa_issued', color='country')
fig.update_layout(
    title_text="Top 10 countries with most issued visa in 2017",
    xaxis_title='Country',
    yaxis_title='Number of visa issued',
    legend=dict(
        title='Country'
    ),
    margin=dict(t=80, r=200, b=80, l=60)
)
fig.write_html('/opt/spark/output/visa_number_in_japan_by_country_2017.html')


# --- VISUALISATION 3: Map Animation ---
df_country_year_map = spark.sql("""
    SELECT year, country, sum(number_of_issued_numerical) visa_issued
    FROM japan_visa
    WHERE country not in ('total', 'others')
    and country is not null
    GROUP BY year, country
    ORDER BY year asc
""")

df_country_year_map = df_country_year_map.toPandas()

fig = px.choropleth(df_country_year_map, locations='country',
                    color='visa_issued',
                    hover_name='country',
                    animation_frame='year',
                    range_color=[0, 1000000], 
                    color_continuous_scale=px.colors.sequential.Plasma,
                    locationmode='country names',
                    title='Yearly visa issued by countries'
                    )

fig.write_html('/opt/spark/output/visa_number_in_japan_year_map.html')

spark.stop()