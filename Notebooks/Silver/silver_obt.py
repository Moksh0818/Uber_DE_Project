# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###Stream Rides Transformation
# MAGIC

# COMMAND ----------

df=spark.sql("select * from uber.bronze.bulk_rides")
rides_schema=StructType([StructField('ride_id', StringType(), True), StructField('confirmation_number', StringType(), True), StructField('passenger_id', StringType(), True), StructField('driver_id', StringType(), True), StructField('vehicle_id', StringType(), True), StructField('pickup_location_id', StringType(), True), StructField('dropoff_location_id', StringType(), True), StructField('vehicle_type_id', LongType(), True), StructField('vehicle_make_id', LongType(), True), StructField('payment_method_id', LongType(), True), StructField('ride_status_id', LongType(), True), StructField('pickup_city_id', LongType(), True), StructField('dropoff_city_id', LongType(), True), StructField('cancellation_reason_id', LongType(), True), StructField('passenger_name', StringType(), True), StructField('passenger_email', StringType(), True), StructField('passenger_phone', StringType(), True), StructField('driver_name', StringType(), True), StructField('driver_rating', DoubleType(), True), StructField('driver_phone', StringType(), True), StructField('driver_license', StringType(), True), StructField('vehicle_model', StringType(), True), StructField('vehicle_color', StringType(), True), StructField('license_plate', StringType(), True), StructField('pickup_address', StringType(), True), StructField('pickup_latitude', DoubleType(), True), StructField('pickup_longitude', DoubleType(), True), StructField('dropoff_address', StringType(), True), StructField('dropoff_latitude', DoubleType(), True), StructField('dropoff_longitude', DoubleType(), True), StructField('distance_miles', DoubleType(), True), StructField('duration_minutes', LongType(), True), StructField('booking_timestamp', TimestampType(), True), StructField('pickup_timestamp', StringType(), True), StructField('dropoff_timestamp', StringType(), True), StructField('base_fare', DoubleType(), True), StructField('distance_fare', DoubleType(), True), StructField('time_fare', DoubleType(), True), StructField('surge_multiplier', DoubleType(), True), StructField('subtotal', DoubleType(), True), StructField('tip_amount', DoubleType(), True), StructField('total_fare', DoubleType(), True), StructField('rating', DoubleType(), True)])


# COMMAND ----------

# DBTITLE 1,Fix NameError: Import from_json
from pyspark.sql.functions import from_json, col

df= spark.read.table("uber.bronze.rides_raw")
df_parsed=df.withColumn("parsed_rides",from_json(col("rides"),rides_schema)).select("parsed_rides.*")
display(df_parsed)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.bronze.stg_rides

# COMMAND ----------

# MAGIC %md
# MAGIC #Jinja Template for OBT

# COMMAND ----------

pip install Jinja2

# COMMAND ----------

# DBTITLE 1,Fix table/alias names and select clause in jinja_config
jinja_config = [
    {
        "table" : "uber.bronze.stg_rides stg_rides",
        "select" : 'stg_rides.ride_id, stg_rides.confirmation_number, stg_rides.passenger_id, stg_rides.driver_id, stg_rides.vehicle_id, stg_rides.pickup_location_id, stg_rides.dropoff_location_id, stg_rides.vehicle_type_id, stg_rides.vehicle_make_id, stg_rides.payment_method_id, stg_rides.ride_status_id, stg_rides.pickup_city_id, stg_rides.dropoff_city_id, stg_rides.cancellation_reason_id, stg_rides.passenger_name, stg_rides.passenger_email, stg_rides.passenger_phone, stg_rides.driver_name, stg_rides.driver_rating, stg_rides.driver_phone, stg_rides.driver_license, stg_rides.vehicle_model, stg_rides.vehicle_color, stg_rides.license_plate, stg_rides.pickup_address, stg_rides.pickup_latitude, stg_rides.pickup_longitude, stg_rides.dropoff_address, stg_rides.dropoff_latitude, stg_rides.dropoff_longitude, stg_rides.distance_miles, stg_rides.duration_minutes, stg_rides.booking_timestamp, stg_rides.pickup_timestamp, stg_rides.dropoff_timestamp, stg_rides.base_fare, stg_rides.distance_fare, stg_rides.time_fare, stg_rides.surge_multiplier, stg_rides.subtotal, stg_rides.tip_amount, stg_rides.total_fare, stg_rides.rating',
        "where" : ""
    },
    {
        "table" : "uber.bronze.map_vehicle_makes map_vehicle_makes",
        "select" : "map_vehicle_makes.vehicle_make",
        "where" : "",
        "on" : "stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id"
    },
    {
        "table" : "uber.bronze.map_vehicle_types map_vehicle_types",
        "select" : "map_vehicle_types.vehicle_type,map_vehicle_types.description,map_vehicle_types.base_rate,map_vehicle_types.per_mile,map_vehicle_types.per_minute",
        "where" : "",
        "on" : "stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id"
    },
    {
        "table" : "uber.bronze.map_ride_statuses map_ride_statuses",
        "select" : "map_ride_statuses.ride_status",
        "where" : "",
        "on" : "stg_rides.ride_status_id = map_ride_statuses.ride_status_id"
    },
    {
        "table" : "uber.bronze.map_payment_methods map_payment_methods",
        "select" : "map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth",
        "where" : "",
        "on" : "stg_rides.payment_method_id = map_payment_methods.payment_method_id"
    },
    {
        "table" : "uber.bronze.map_cities map_cities",
        "select" : "map_cities.city as pickup_city, map_cities.state, map_cities.region, map_cities.updated_at as city_updated_at",
        "where" : "",
        "on" : "stg_rides.pickup_city_id = map_cities.city_id"
    },
    {
        "table" : "uber.bronze.map_cancellation_reasons map_cancellation_reasons",
        "select" : "map_cancellation_reasons.cancellation_reason",
        "where" : "",
        "on" : "stg_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id"
    }
]

# COMMAND ----------

# DBTITLE 1,Fix TemplateSyntaxError: Jinja syntax correction
from jinja2 import Template
jinja_str="""
   select 
      {% for config in jinja_config %}
        {{config.select}}
          {% if not loop.last %}
            ,
          {% endif %}
      {% endfor %}
    from
      {% for config in jinja_config %}
      {% if loop.first %}
        {{config.table}}
      {% else %}
          left join {{config.table}} on {{config.on}}
       {% endif %}
      {% endfor %} 
    
       {% for config in jinja_config %}
         {% if loop.first %}
          {% if config.where != "" %}
           where
          {% endif %}
         {% endif %} 
        
         {{config.where}}
           {% if not loop.last %}
              {% if config.where != "" %}
              AND
              {% endif %}
           {% endif %}
       {% endfor %}            
"""
template=Template(jinja_str)
rendered_template = template.render(jinja_config=jinja_config)
print(rendered_template)

# COMMAND ----------

# DBTITLE 1,Print generated SQL before executing
spark.sql(rendered_template)

# COMMAND ----------

display(spark.sql(rendered_template))

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_timestamp()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.silver.silver_obt

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC    distinct(passenger_id) passenger_id,
# MAGIC    passenger_name,
# MAGIC    passenger_email,
# MAGIC    passenger_phone
# MAGIC from
# MAGIC   uber.silver.silver_obt   

# COMMAND ----------

df= spark.read.table("uber.silver.silver_obt")
df= df.select( "passenger_id","passenger_name", "passenger_email", "passenger_phone")
df= df.dropDuplicates(subset=['passenger_id'])
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.gold.dim_passenger

# COMMAND ----------

df= spark.read.table("uber.silver.silver_obt")
df= df.select( "pickup_city_id","pickup_city","city_updated_at","region","state")
df= df.dropDuplicates(subset=['pickup_city_id'])
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.gold.dim_location

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into uber.silver.silver_obt(pickup_city_id,pickup_city,city_updated_at,region,state)
# MAGIC values(1,"New York",current_timestamp(),"North","New York")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into uber.silver.silver_obt(pickup_city_id,pickup_city,city_updated_at,region,state)
# MAGIC values(1,"New Jersey",current_timestamp(),"North","New Jersey")

# COMMAND ----------

df= spark.read.table("uber.silver.silver_obt")
df= df.select("ride_id","distance_miles","duration_minutes","base_fare","distance_fare","time_fare","surge_multiplier","total_fare","tip_amount","rating","base_rate","per_mile","per_minute")
df= df.dropDuplicates(subset=['ride_id'])
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.gold.fact

# COMMAND ----------

# MAGIC %md
# MAGIC #Gold Layer Testing

# COMMAND ----------

# MAGIC %sql
# MAGIC select fact.ride_id,fact.base_fare,dim.region from uber.gold.fact fact LEFT JOIN uber.gold.dim_location dim
# MAGIC on
# MAGIC fact.pickup_city_id=dim.pickup_city_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.gold.dim_passenger

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC select fact.ride_id,fact.base_fare,dim.passenger_name from uber.gold.fact fact LEFT JOIN uber.gold.dim_passenger dim
# MAGIC on
# MAGIC fact.passenger_id=dim.passenger_id

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC select * from uber.gold.dim_payment

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC select fact.ride_id,fact.base_fare,dim.payment_method,dim.payment_method_id from uber.gold.fact fact LEFT JOIN uber.gold.dim_payment
# MAGIC dim
# MAGIC on
# MAGIC fact.payment_method_id=dim.payment_method_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.gold.dim_vehicle

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC select fact.ride_id,fact.base_fare,dim.vehicle_type,dim.vehicle_type_id,vehicle_make,vehicle_model from uber.gold.fact fact LEFT JOIN uber.gold.dim_vehicle dim
# MAGIC on
# MAGIC fact.vehicle_id=dim.vehicle_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.gold.dim_booking

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC select fact.ride_id,fact.base_fare,dim.confirmation_number,dim.dropoff_location_id,dim.dropoff_address,dim.dropoff_city_id,dim.pickup_location_id from uber.gold.fact fact LEFT JOIN uber.gold.dim_booking dim
# MAGIC on
# MAGIC fact.ride_id=dim.ride_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.gold.dim_driver
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC select fact.ride_id,fact.base_fare,dim.driver_name,dim.driver_phone,dim.driver_rating,dim.driver_license from uber.gold.fact fact LEFT JOIN uber.gold.dim_driver dim
# MAGIC on
# MAGIC fact.driver_id=dim.driver_id

# COMMAND ----------

