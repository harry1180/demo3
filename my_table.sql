CREATE TABLE `my_table` (
  `col1` STRING NOT NULL COMMENT 'Description for col1',  -- Add comments for columns (optional)
  `col2` INT NOT NULL,
  `col3` ARRAY<STRING> COMMENT 'Array of strings'  -- Example of a complex data type
)
PARTITIONED BY (`partition_col` STRING)  -- Add partitioning (optional)
