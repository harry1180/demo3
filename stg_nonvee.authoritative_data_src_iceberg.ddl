CREATE TABLE IF NOT EXISTS stg_nonvee.authoritative_data_${opco}_src (
  metername STRING COMMENT 'metername',
  utildeviceid STRING COMMENT 'Util Device ID',
  macid STRING COMMENT 'Mac ID',
  interval_reading ARRAY<STRUCT<
    intervallength STRING,
    starttime STRING,
    endtime STRING,
    numberintervals STRING,
    IntervalReadData ARRAY<STRUCT<
      endtime STRING,
      blocksequencenumber STRING,
      gatewaycollectedtime STRING,
      intervalsequencenumber STRING,
      interval ARRAY<STRUCT<
        channel STRING,
        rawvalue STRING,
        value STRING,
        uom STRING,
        blockendvalue STRING
      >>
    >>
  >> COMMENT 'interval_reading'
)
PARTITIONED BY (part_date STRING)
USING iceberg
LOCATION '${HDFS_DATA_WORK}/util/intervals/authoritative/${opco}/authoritative_data_${opco}_src'
TBLPROPERTIES (
  'partitioning.type' = 'hash',  -- or 'range' based on your partitioning strategy
  'partitioning.field' = 'part_date',
  'format' = 'parquet',
  'iceberg.schema.evolution.mode' = 'ADD_REQUIRED_FIELD',  -- or 'ADD_NOT_NULL' as needed
  'xmlinput.end' = '</MeterData>',
  'xmlinput.start' = '<MeterData '
);
