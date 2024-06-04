from pyspark.sql import SparkSession

# Initialize Spark session with Iceberg configuration
spark = SparkSession.builder \
    .appName("IcebergExample") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "path/to/warehouse") \
    .getOrCreate()

# Create dummy data
data = [
    (1, "Alice", 29),
    (2, "Bob", 31),
    (3, "Catherine", 25)
]
columns = ["id", "name", "age"]
df = spark.createDataFrame(data, columns)

# Save DataFrame as an Iceberg table
df.write \
    .format("iceberg") \
    .mode("overwrite") \
    .save("spark_catalog.default.my_iceberg_table")

# Read and show the data from the Iceberg table
df_read = spark.read.format("iceberg").load("spark_catalog.default.my_iceberg_table")
df_read.show()

# Stop the Spark session
spark.stop()
