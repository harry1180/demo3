DROP TABLE IF EXISTS stg_nonvee.authoritative_data_${opco}_stg_joined;

CREATE TABLE IF NOT EXISTS stg_nonvee.authoritative_data_${opco}_stg_joined (
    auth_serialnumber STRING,
    auth_intervallength STRING,
    auth_starttimeperiod STRING,
    auth_endtimeperiod STRING,
    auth_interval_epoch STRING,
    auth_timezone_cd STRING,
    auth_int_gatewaycollectedtime STRING,
    auth_aep_raw_uom STRING,
    auth_value STRING,
    auth_aep_opco STRING,
    auth_aep_channel_id STRING,
    auth_aep_derived_uom STRING,
    serialnumber STRING,
    endtimeperiod STRING,
    value DOUBLE,
    aep_raw_value DOUBLE,
    hdp_insert_dttm TIMESTAMP,
    aep_derived_uom STRING,
    auth_aep_usage_dt STRING,
    aep_usage_dt STRING,
    aep_channel_id STRING,
    aep_meter_bucket STRING
)
USING iceberg
PARTITIONED BY (part_date STRING)
LOCATION '${HDFS_DATA_WORK}/util/intervals/authoritative/${opco}/authoritative_data_${opco}_stg_joined'
TBLPROPERTIES (
    'format' = 'orc',
    'orc.bloom.filter.columns' = 'auth_serialnumber',
    'orc.bloom.filter.fpp' = '0.05',
    'orc.compress' = 'SNAPPY',
    'iceberg.schema.evolution.mode' = 'ADD_REQUIRED_FIELD'
);
