from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
import re  # for parsing DDL (replace with a suitable library if needed)

def create_table_from_ddl(spark, table_name, ddl_path):
  """
  Creates a table in Spark by reading the schema from a DDL file.

  Args:
      spark (SparkSession): SparkSession object.
      table_name (str): Name of the table to create.
      ddl_path (str): Path to the DDL file defining the table schema.
  """

  # 1. Read DDL from file
  with open(ddl_path, "r") as f:
    ddl_text = f.read()

  # 2. Parse DDL to extract schema (replace with a more robust parsing library if needed)
  # This example uses regular expressions for basic parsing. Consider using libraries like `sqlparse` for more complex DDL formats.
  schema_regex = r"CREATE TABLE\s+`(.+?`)\s?\((.+?)\)"
  match = re.search(schema_regex, ddl_text, re.IGNORECASE)
  if match:
    table_name = match.group(1)  # Extract table name
    column_defs = match.group(2).split(",")  # Split column definitions

    # Build schema from column definitions
    schema_fields = []
    for col_def in column_defs:
      col_def = col_def.strip()  # Remove leading/trailing spaces
      col_name, col_type = col_def.split(" ")  # Split column name and type

      # Add more logic here to handle complex data types (e.g., nested structs)
      if col_type.lower() == "string":
        data_type = StringType()
      elif col_type.lower() == "int":
        data_type = IntegerType()
      else:
        # Handle other data types or raise an error
        raise ValueError(f"Unsupported data type: {col_type}")

      schema_fields.append(StructField(col_name, data_type, True))

    schema = StructType(schema_fields)
  else:
    raise ValueError("Failed to parse DDL schema.")

  # 3. Create table with the extracted schema
  spark.sql(f"CREATE TABLE {table_name} USING {schema}")

  # 4. (Optional) Load data from source (replace with your data source logic)
  # This script focuses on schema creation. Implement your data loading logic here.

  print(f"Table '{table_name}' created successfully!")

# Configure SparkSession
spark = SparkSession.builder.appName("CreateTableFromDDL").getOrCreate()

# Specify table name and DDL file path (replace with your values)
table_name = "my_table"
ddl_path = "s3://your-bucket/path/to/ddl.sql"  # Replace with S3 or local path

create_table_from_ddl(spark, table_name, ddl_ddl_path)

spark.stop()
