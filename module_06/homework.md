# Question 1

`rpk version`

# Question 2 Create topic

```
rpk topic create test-topic
```

# Question 3 Connection kafka server

```python
import json
import time

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()
```

# Question 4 Sending data to stream

```python
t0 = time.time()

topic_name = 'test-topic'

for i in range(10):
    message = {'number': i}
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")
    time.sleep(0.05)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
```

# Question 5

```python
green_df = pd.read_csv("data/ny_taxi/green_tripdata_2019-10.csv.gz")
topic_name = 'green-trips'

t0 = time.time()
for row in green_df[['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID',
    'passenger_count', 'trip_distance', 'tip_amount']].itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    producer.send(topic_name, value=row_dict)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
```

# Pyspark

```python
import pyspark
from pyspark.sql import SparkSession

pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("GreenTripsConsumer") \
    .config("spark.jars.packages", kafka_jar_package) \
    .getOrCreate()

green_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "green-trips") \
    .option("startingOffsets", "earliest") \
    .load()

def peek(mini_batch, batch_id):
    first_row = mini_batch.take(1)

    if first_row:
        print(first_row[0])

query = green_stream.writeStream.foreachBatch(peek).start()
```

# Question 6. Parsing the data

```python
from pyspark.sql import types
from pyspark.sql import functions as F

schema = types.StructType() \
    .add("lpep_pickup_datetime", types.StringType()) \
    .add("lpep_dropoff_datetime", types.StringType()) \
    .add("PULocationID", types.IntegerType()) \
    .add("DOLocationID", types.IntegerType()) \
    .add("passenger_count", types.DoubleType()) \
    .add("trip_distance", types.DoubleType()) \
    .add("tip_amount", types.DoubleType())

green_stream = green_stream \
  .select(F.from_json(F.col("value").cast('STRING'), schema).alias("data")) \
  .select("data.*")
```

# Question 7. Most popular destination

producer
```python
import pandas as pd
import json
import time
from datetime import datetime
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()

green_df = pd.read_csv("data/ny_taxi/green_tripdata_2019-10.csv.gz", chunksize=10_000)
topic_name = 'green-trips'

for chunk in green_df:
    chunk['timestamp'] = pd.to_datetime(datetime.now())
    chunk['timestamp'] = chunk['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    for row in chunk[['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID',
        'passenger_count', 'trip_distance', 'tip_amount', 'timestamp']].itertuples(index=False):
        row_dict = {col: getattr(row, col) for col in row._fields}
        producer.send(topic_name, value=row_dict)

producer.flush()
```

consumer
```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F

pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("GreenTripsConsumer") \
    .config("spark.jars.packages", kafka_jar_package) \
    .getOrCreate()

green_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "green-trips") \
    .option("startingOffsets", "earliest") \
    .load()

schema = types.StructType() \
    .add("lpep_pickup_datetime", types.StringType()) \
    .add("lpep_dropoff_datetime", types.StringType()) \
    .add("timestamp", types.StringType()) \
    .add("PULocationID", types.IntegerType()) \
    .add("DOLocationID", types.IntegerType()) \
    .add("passenger_count", types.DoubleType()) \
    .add("trip_distance", types.DoubleType()) \
    .add("tip_amount", types.DoubleType())

popular_destinations = green_stream \
    .select(F.from_json(F.col("value").cast('STRING'), schema).alias("data")) \
    .select("data.*") \
    .groupBy(F.window(F.col("timestamp"), "5 minutes"), F.col("DOLocationID")) \
    .agg(F.expr("count(*) as record_count")) \
    .orderBy(F.col("record_count").desc())

query = popular_destinations \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
```
