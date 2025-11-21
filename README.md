

*** Worldwide Earthquake Events API — Medallion Architecture Processing (Bronze ➜ Silver ➜ Gold)***

***This project demonstrates an end-to-end Azure Fabric Medallion Architecture pipeline using the USGS Worldwide Earthquake GeoJSON API.***

_____________________________________________________________________________________________________________________________________________________________________________

***Data Pipeline***

<img width="1305" height="567" alt="Data Pipeline" src="https://github.com/user-attachments/assets/462b69ea-3834-4689-99fe-5ec6a23cb54e" />

<img width="1366" height="768" alt="BronzeNotebook Parameters" src="https://github.com/user-attachments/assets/efd7f01c-1a92-433b-843e-f714f22ca26a" />

<img width="1366" height="768" alt="Silver Notebook Parameters" src="https://github.com/user-attachments/assets/a098aeaa-b34d-450d-a64c-fcd32aa73704" />

<img width="1366" height="768" alt="Gold Notebook Parameters" src="https://github.com/user-attachments/assets/aa8bc332-161f-4da8-98ff-b9868248e510" />

***In case we want to make our Pipelines Dynamic, we can pass dynamic arguments for live data for current  dates***

start_date= @formatDateTime(adddays(utcnow(),-1),'yyyy-MM-dd')

end_date= @formatDateTime(utcnow()),'yyyy-MM-dd')


_____________________________________________________________________________________________________________________________________________________________________________

***We ingest raw earthquake events into the Bronze Layer, transform and standardize in Silver, and enrich the data with geolocation and classification in Gold.***
________________________________________
***Data Refresh Strategy***

***🔹 Dynamic Date Refresh (Live API Call) ***

This works only when an active API Subscription / live data is available. Inour case live data was not available.

from datetime import date, timedelta

start_date = date.today() - timedelta(days=5)  # Fetch 5 days of past events

end_date = date.today() - timedelta(days=1)

print(f"start_date: {start_date}")

print(f"end_date: {end_date}")
________________________________________
***🔹 Static Data (Used Currently) ***

As Live data for 2025-11-20 is not available — so we manually provide dates for historical reference:

start_date = '2014-01-01'

end_date = '2014-01-10'
________________________________________
***Bronze Layer — API Data Ingestion***

Reads GeoJSON from USGS Earthquake API, extracts features and stores JSON into Fabric Lakehouse:

import requests
import json

url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"
response = requests.get(url)

if response.status_code == 200:
    data = response.json()['features']
    file_path = f'/lakehouse/default/Files/{start_date}_earthquake_data.json'

    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)

    print(f"Data successfully saved to {file_path}")
else:
    print("Failed to fetch data. Status code:", response.status_code)


***Output Storage Example***

/lakehouse/default/Files/2014-01-01_earthquake_data.json

<img width="1366" height="598" alt="json data" src="https://github.com/user-attachments/assets/013c5050-4dc1-451e-b18f-cd967d233195" />

________________________________________

***Silver Layer — Data Standardization***

Flatten GeoJSON structure and convert timestamps:

from pyspark.sql.functions import *
from pyspark.sql.types import *

df = spark.read.option("multiline", "true").json(
    f"Files/{start_date}_earthquake_data.json"
)

df = df.select(
    'id',
    
    col('geometry.coordinates')[0].alias('longitude'),
    
    col('geometry.coordinates')[1].alias('latitude'),
  
    col('geometry.coordinates')[2].alias('elevation'),
    
    col('properties.title').alias('title'),
    
    col('properties.place').alias('place_description'),
    
    col('properties.sig').alias('sig'),
    
    col('properties.mag').alias('mag'),
    
    col('properties.magType').alias('magType'),
    
    col('properties.time').alias('time'),
    
    col('properties.updated').alias('updated')
)

df = df.withColumn('time', (col('time')/1000).cast(TimestampType())) \
       
       .withColumn('updated', (col('updated')/1000).cast(TimestampType()))

df.write.mode('append').saveAsTable('earthquake_events_silver')

***Silver layer now contains clean tabular data. ***
<img width="1366" height="585" alt="silverdata" src="https://github.com/user-attachments/assets/7ca90378-c49b-4e51-9bca-bcb5466b811a" />

________________________________________
***Gold Layer — Data Enrichment***

✔ Reverse Geocoding → Country code
✔ Significance Classification (Low, Moderate, High)

from pyspark.sql.functions import *
from pyspark.sql.types import *
import reverse_geocoder as rg

df = spark.read.table("earthquake_events_silver") \
               .filter(col('time') > start_date)

def get_country_code(lat, lon):
    return rg.search((float(lat), float(lon)))[0]['cc']

get_country_code_udf = udf(get_country_code, StringType())

df_with_location = df.withColumn(
    "country_code", get_country_code_udf(col("latitude"), col("longitude"))
)

df_gold = df_with_location.withColumn(
    'sig_class',
    
    when(col("sig") < 100, "Low")
    
    .when(col("sig").between(100, 499), "Moderate")
    
    .otherwise("High")
)

df_gold.write.mode('append').saveAsTable('earthquake_events_gold')

<img width="1366" height="594" alt="Gold table" src="https://github.com/user-attachments/assets/996054fb-fc53-4df4-8d04-35a112891a44" />

_______________________________________________________________________________________________________________________________________________________________
***Power BI reporting on the basis final layer Gold data***

<img width="890" height="497" alt="BI Report page 1" src="https://github.com/user-attachments/assets/973bff53-a000-4c38-830f-4f5bb29b099f" />

<img width="886" height="498" alt="BI Report page 2" src="https://github.com/user-attachments/assets/ad8d3b9e-b053-4963-b480-047234afcf7a" />

