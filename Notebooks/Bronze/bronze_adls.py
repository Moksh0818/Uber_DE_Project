# Databricks notebook source
# DBTITLE 1,Download and Preview Files
import pandas as pd

files=[
{"file":"map_cities"},    
{"file":"map_cancellation_reasons"},
{"file":"map_payment_methods"},
{"file":"map_ride_statuses"},
{"file":"map_vehicle_makes"},
{"file":"map_vehicle_types"}
]

for file in files:
   print(f"Processing: {file['file']}")
   url=f"https://dlmokshuberproject.blob.core.windows.net/raw/ingestion/{file['file']}.json?sp=r&st=2026-03-20T17:10:35Z&se=2026-03-21T01:25:35Z&spr=https&sv=2024-11-04&sr=c&sig=eTyRwOEl7M1GqfjjoB%2BsGScEb%2FAHr8vhDuvGah1iO4o%3D"

   df=pd.read_json(url)
   df_spark=spark.createDataFrame(df)

   df_spark.write.format("delta")\
       .mode("overwrite")\
       .option("overwriteSchema","true")\
       .saveAsTable(f"uber.bronze.{file['file']}")

# COMMAND ----------

url=f"https://dlmokshuberproject.blob.core.windows.net/raw/ingestion/bulk_rides.json?sp=r&st=2026-03-20T04:08:38Z&se=2026-03-20T12:23:38Z&spr=https&sv=2024-11-04&sr=c&sig=U1QX%2FRzAUhXs6IQ5h3OQFbGWIPbvGb61eXeyLB9YO0I%3D"

df=pd.read_json(url)
df_spark=spark.createDataFrame(df)
if not spark.catalog.tableExists("uber.bronze.bulk_rides"):
    df_spark.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable(f"uber.bronze.bulk_rides")
    print("This will not run more than 1 time")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.bronze.map_cities

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.bronze.bulk_rides

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.bronze.rides_raw

# COMMAND ----------

