import pycountry
import pycountry_convert as pcc
from fuzzywuzzy import process
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# 1. Initialize Spark
spark = SparkSession.builder.appName('Japan Visa ETL').getOrCreate()

# 2. Read Raw Data
df = spark.read.csv('/opt/spark/input/visa_number_in_japan.csv', header=True, inferSchema=True)

# 3. Clean Column Names
new_column_names = [col_name.replace(' ', '_')
                    .replace('/', '')
                    .replace('.', '')
                    .replace(',', '')
                    for col_name in df.columns]
df = df.toDF(*new_column_names)

# 4. Drop Nulls and Select Columns
df = df.dropna(how='all')
df = df.select('year', 'country', 'number_of_issued_numerical')

# 5. Define Cleaning Functions
def correct_country_name(name, threshold=85):
    countries = [country.name for country in pycountry.countries]
    corrected_name, score = process.extractOne(name, countries)
    if score >= threshold:
        return corrected_name
    return name

def get_continent_name(country_name):
    try:
        country_code = pcc.country_name_to_country_alpha2(country_name, cn_name_format='default')
        continent_code = pcc.country_alpha2_to_continent_code(country_code)
        return pcc.convert_continent_code_to_continent_name(continent_code)
    except:
        return None

# 6. Apply UDFs (Fuzzy Match & Continent)
correct_country_name_udf = udf(correct_country_name, StringType())
df = df.withColumn('country', correct_country_name_udf(df['country']))

# 7. Apply Manual Dictionary Fixes
country_corrections = {
    'Andra': 'Russia',
    'Antigua Berbuda': 'Antigua and Barbuda',
    'Barrane': 'Bahrain',
    'Brush': 'Bhutan',
    'Komoro': 'Comoros',
    'Benan': 'Benin',
    'Kiribass': 'Kiribati',
    'Gaiana': 'Guyana',
    'Court Jiboire': "Côte d'Ivoire",
    'Lesot': 'Lesotho',
    'Macau travel certificate': 'Macao',
    'Moldoba': 'Moldova',
    'Naure': 'Nauru',
    'Nigail': 'Niger',
    'Palao': 'Palau',
    'St. Christopher Navis': 'Saint Kitts and Nevis',
    'Santa Principa': 'Sao Tome and Principe',
    'Saechel': 'Seychelles',
    'Slinum': 'Saint Helena',
    'Swaji Land': 'Eswatini',
    'Torque menistan': 'Turkmenistan',
    'Tsubaru': 'Zimbabwe',
    'Kosovo': 'Kosovo'
}
df = df.replace(country_corrections, subset='country')

continent_udf = udf(get_continent_name, StringType())
df = df.withColumn('continent', continent_udf(df['country']))

# 8. SAVE the Cleaned Data (The "Handshake" with the next file)
# We save to a single CSV file for easier reading in the next step
print("Writing cleaned data to disk...")
df.write.csv("/opt/spark/output/visa_number_in_japan_cleaned.csv", header=True, mode='overwrite')

spark.stop()