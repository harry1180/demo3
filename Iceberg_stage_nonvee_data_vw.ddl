-- Create the new Iceberg table
CREATE TABLE stg_nonvee.interval_data_files_oh_stg_vw_iceberg (
    serialnumber STRING,
    source STRING,
    aep_devicecode STRING,
    isvirtual_meter STRING,
    timezoneoffset STRING,
    aep_premise_nb STRING,
    aep_service_point STRING,
    aep_srvc_dlvry_id STRING,
    name_register STRING,
    isvirtual_register STRING,
    aep_derived_uom STRING,
    aep_raw_uom STRING,
    aep_srvc_qlty_idntfr STRING,
    aep_channel_id STRING,
    aep_sec_per_intrvl INT,
    aep_meter_alias STRING,
    aep_meter_program STRING,
    aep_usage_type STRING,
    aep_timezone_cd STRING,
    endtimeperiod STRING,
    starttimeperiod STRING,
    value DOUBLE,
    aep_raw_value STRING,
    scalarfloat STRING,
    aep_acct_cls_cd STRING,
    aep_acct_type_cd STRING,
    aep_mtr_pnt_nb STRING,
    aep_tarf_pnt_nb STRING,
    aep_comp_mtr_mltplr DOUBLE,
    aep_endtime_utc TIMESTAMP,
    aep_mtr_removal_ts STRING,
    aep_mtr_install_ts STRING,
    aep_city STRING,
    aep_zip STRING,
    aep_state STRING,
    hdp_insert_dttm TIMESTAMP,
    hdp_update_dttm TIMESTAMP,
    hdp_update_user STRING,
    authority STRING,
    aep_usage_dt STRING,
    data_type STRING,
    aep_opco STRING,
    aep_meter_bucket STRING
)
USING iceberg
LOCATION '${HDFS_DATA_WORK}/util/intervals/nonvee/oh/stg_vw_iceberg'
TBLPROPERTIES (
    'write.format.default'='parquet',
    'write.data_compression'='SNAPPY',
    'write.partitioning.default'='part_date'
);

-- Insert data into the new Iceberg table
INSERT INTO stg_nonvee.interval_data_files_oh_stg_vw_iceberg
SELECT
    serialnumber,
    'nonvee-hes' as source,
    aep_devicecode,
    'N' as isvirtual_meter,
    interval_epoch as timezoneoffset,
    aep_premise_nb,
    IF(
        aep_premise_nb IS NOT NULL AND aep_tarf_pnt_nb IS NOT NULL AND aep_mtr_pnt_nb IS NOT NULL,
        concat(aep_premise_nb, '-', aep_tarf_pnt_nb, '-', aep_mtr_pnt_nb),
        NULL
    ) as aep_service_point,
    aep_srvc_dlvry_id,
    name_register,
    'N' as isvirtual_register,
    aep_derived_uom,
    aep_raw_uom,
    aep_srvc_qlty_idntfr,
    aep_channel_id,
    (cast(intervallength as int) * 60) as aep_sec_per_intrvl,
    NULL as aep_meter_alias,
    aep_meter_program,
    'interval' as aep_usage_type,
    'US/Eastern' as aep_timezone_cd,
    endtime as endtimeperiod,
    starttime as starttimeperiod,
    CASE
        WHEN value_mltplr_flg = 'Y' THEN (value * bill_cnst)
        ELSE value
    END as value,
    aep_raw_value,
    bill_cnst as scalarfloat,
    aep_acct_cls_cd,
    aep_acct_type_cd,
    aep_mtr_pnt_nb,
    aep_tarf_pnt_nb,
    cast(aep_comp_mtr_mltplr as double) as aep_comp_mtr_mltplr,
    unix_timestamp(concat(endtime, interval_epoch), 'yyyy-MM-dd''T''HH:mm:ssXXX') as aep_endtime_utc,
    aep_mtr_removal_ts,
    aep_mtr_install_ts,
    aep_city,
    aep_zip,
    aep_state,
    current_timestamp as hdp_insert_dttm,
    current_timestamp as hdp_update_dttm,
    hdp_update_user,
    substr(aep_premise_nb, 1, 2) as authority,
    substr(starttime, 0, 10) as aep_usage_dt,
    'new' as data_type,
    aep_opco,
    aep_meter_bucket
FROM
    stg_nonvee.interval_data_files_oh_stg;
