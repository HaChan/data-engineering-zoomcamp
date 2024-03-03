from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("fhv_tripdata").getOrCreate()

# Question 1
# Install Spark and PySpark
print(spark.version)

# Question 2
# average size of the Parquet
fhv_df = spark.read.csv("data/ny_taxi/fhv_tripdata_2019-10.csv.gz", header=True, inferSchema=True)
fhv_df_repartitioned = df.repartition(6)
fhv_df_repartitioned.write.mode("overwrite").parquet("data/ny_taxi/fhv_parquets")

# Question 3
# How many taxi trips were there on the 15th of October
fhv_df.createOrReplaceTempView("fhv")
print(spark.sql("select count(*) from fhv where DATE(pickup_datetime) = '2019-10-15'").show())

# Question 4
# Length of the longest trip in the dataset in hours
print(spark.sql("select max(UNIX_TIMESTAMP(dropOff_datetime) - UNIX_TIMESTAMP(pickup_datetime))/3600 from fhv").show())

# Question 6
# Least frequent pickup location zone
zone_df = spark.read.csv("data/taxi+_zone_lookup.csv", header=True, inferSchema=True)
zone_df.createOrReplaceTempView("zones")
print(spark.sql("""
    SELECT any_value(Zone), PUlocationID, count(1) as pickup_num
    FROM Fhv
    JOIN Zones on fhv.PUlocationID = zones.LocationID
    GROUP by PUlocationID
    ORDER by pickup_num asc
    LIMIT 5
""").show())

spark.stop()
