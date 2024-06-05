DROP TABLE IF EXISTS stg_nonvee.authoritative_data_${opco}_stg;

CREATE EXTERNAL TABLE stg_nonvee.authoritative_data_${opco}_stg (
    serialnumber STRING,
    intervallength STRING,
    starttimeperiod STRING,
    endtimeperiod STRING,
    interval_epoch STRING,
    int_gatewaycollectedtime STRING,
    aep_raw_uom STRING,
    aep_raw_value STRING,
    value STRING,
    aep_opco STRING,
    aep_channel_id STRING,
    aep_usage_dt STRING,
    aep_derived_uom STRING,
    aep_meter_bucket STRING
)
USING iceberg
LOCATION '${HDFS_DATA_WORK}/util/intervals/authoritative/${opco}/authoritative_data_${opco}_stg';
