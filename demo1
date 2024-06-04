from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType

# Define table schema (modify as needed)
schema = StructType([
    StructField("id", StringType(), True),
    StructField("data", StringType(), True)
])

# Configure Iceberg table location (replace with your desired location)
table_location = "/path/to/your/iceberg/table/location"

# Create SparkSession
spark = SparkSession.builder \
    .appName("CreateIcebergTable") \
    .getOrCreate()

# Create Iceberg table
spark.sql(f"""
CREATE TABLE USING iceberg
LOCATION '{table_location}'
TBLPROPERTIES (
  'write.format.default'='parquet'
)
""")

# Stop SparkSession
spark.stop()

print("Iceberg table created successfully!")
