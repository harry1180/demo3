DROP TABLE IF EXISTS stg_nonvee.authoritative_data_${opco}_stg_prejoin_filter_tbl;

CREATE TABLE IF NOT EXISTS stg_nonvee.authoritative_data_${opco}_stg_prejoin_filter_tbl (
    serialnumber STRING,
    endtimeperiod STRING,
    value FLOAT,
    aep_raw_value FLOAT,
    hdp_insert_dttm TIMESTAMP,
    aep_derived_uom STRING,
    aep_usage_dt STRING,	
    aep_channel_id STRING,
    aep_meter_bucket STRING
)
USING iceberg
LOCATION '${HDFS_DATA_WORK}/util/intervals/authoritative/${opco}/authoritative_data_${opco}_stg_prejoin_filter'
TBLPROPERTIES (
    'format' = 'orc',
    'orc.bloom.filter.columns' = 'serialnumber',
    'orc.bloom.filter.fpp' = '0.05',
    'orc.compress' = 'SNAPPY',
    'iceberg.schema.evolution.mode' = 'ADD_REQUIRED_FIELD'
);
