CREATE TABLE stg_nonvee.authoritative_data_${opco}_weekly_stg (
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
    aep_meter_bucket STRING,
    run_dt STRING
)
USING iceberg
PARTITIONED BY (run_dt)
LOCATION '${HADOOP_DATA_TRANSFORM}/util/intervals/authoritative/holding/${opco}/authoritative_data_${opco}_weekly_stg';
