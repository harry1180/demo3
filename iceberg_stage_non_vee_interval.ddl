CREATE TABLE stg_nonvee.interval_data_files_oh_stg (
    filename STRING,
    aep_opco STRING,
    serialnumber STRING,
    utildeviceid STRING,
    macid STRING,
    intervallength STRING,
    blockstarttime STRING,
    blockendtime STRING,
    numberintervals STRING,
    starttime STRING,
    endtime STRING,
    interval_epoch STRING,
    int_gatewaycollectedtime STRING,
    int_blocksequencenumber STRING,
    int_intervalsequencenumber STRING,
    channel STRING,
    aep_raw_value STRING,
    value STRING,
    aep_raw_uom STRING,
    aep_derived_uom STRING,
    name_register STRING,
    aep_srvc_qlty_idntfr STRING,
    aep_premise_nb STRING,
    bill_cnst STRING,
    aep_acct_cls_cd STRING,
    aep_acct_type_cd STRING,
    aep_devicecode STRING,
    aep_meter_program STRING,
    aep_srvc_dlvry_id STRING,
    aep_mtr_pnt_nb STRING,
    aep_tarf_pnt_nb STRING,
    aep_comp_mtr_mltplr STRING,
    aep_mtr_removal_ts STRING,
    aep_mtr_install_ts STRING,
    aep_city STRING,
    aep_zip STRING,
    aep_state STRING,
    hdp_update_user STRING,
    part_date STRING,
    value_mltplr_flg STRING,
    aep_meter_bucket STRING
)
USING iceberg
PARTITIONED BY (part_date)----------------DATE
LOCATION '${HDFS_DATA_WORK}/util/intervals/nonvee/oh/stg'--------------NEED TO CHANGE THE PATH
TBLPROPERTIES (
    'write.format.default'='parquet',
    'write.data_compression'='SNAPPY',
    'write.partitioning.default'='part_date',
    'parquet.bloom.filter.columns'='serialnumber',
    'parquet.bloom.filter.fpp'='0.05',
    'parquet.create.index'='true'
);
