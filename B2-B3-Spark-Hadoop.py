from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType

# Create SparkSession
spark = SparkSession.builder \
    .appName("KafkaCreditCardProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# Define Kafka parameters
kafka_bootstrap_servers = "127.0.0.1:9092"
kafka_topic = "credit_card_transactions"

# Define input source from Kafka
lines = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Convert value from Kafka message to string
lines = lines.selectExpr("CAST(value AS STRING) as json_str")

# Define the schema based on your data
schema = StructType([
    StructField("User", IntegerType(), True),
    StructField("Card", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Month", IntegerType(), True),
    StructField("Day", IntegerType(), True),
    StructField("Time", StringType(), True),
    StructField("Amount", StringType(), True),
    StructField("Use Chip", StringType(), True),
    StructField("Merchant Name", StringType(), True),
    StructField("Merchant City", StringType(), True),
    StructField("Merchant State", StringType(), True),
    StructField("Zip", StringType(), True),
    StructField("MCC", IntegerType(), True),
    StructField("Errors?", StringType(), True),
    StructField("Is Fraud?", StringType(), True)
])

# Ensure that json_str has StringType
lines = lines.withColumn("json_str", lines["json_str"].cast(StringType()))

# Parse JSON
df = lines.select(from_json("json_str", schema).alias("data")).select("data.*")

# Filter out rows with Is Fraud? equals "Yes"
df = df.filter(col("Is Fraud?") != "Yes")

# Combine Day, Month, Year into a single column with format dd/mm/yyyy
df = df.withColumn("TransactionDate", expr("format_string('%02d/%02d/%04d', Day, Month, Year)"))

# Convert Amount from dollar to VND (assuming exchange rate is 23000)
df = df.withColumn("Amount", regexp_replace("Amount", "[^\d.]", ""))  # Remove non-numeric characters
df = df.withColumn("Amount", col("Amount").cast(DecimalType(10, 2)))  # Convert to DecimalType
df = df.withColumn("Amount_VND", col("Amount") * 23000.1)

# Drop columns Year, Month, Day, Amount
df = df.drop("Year", "Month", "Day", "Amount")

# Adjust the format of Time
df = df.withColumn("Time", expr("concat(Time, ':00')"))

# Save DataFrame to Hadoop in csv format
hadoop_path = "hdfs://localhost:9000/data_process"
hadoop_checkpoint = "hdfs://localhost:9000/checkpoint"

df.writeStream \
   .format("csv") \
   .outputMode("append") \
   .option("path", hadoop_path) \
   .option("checkpointLocation", hadoop_checkpoint) \
   .start() \
   .awaitTermination()
    
# Show the DataFrame
df.writeStream.outputMode("append").format("console").start().awaitTermination()

