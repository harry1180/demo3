#!/bin/bash
#/**------------------------------------------------------------------------------------------**/
#/**          AMERICAN ELECTRIC POWER – Underground Network Project                           **/
#/**------------------------------------------------------------------------------------------**/
#/**------------------------------------------------------------------------------------------**/

set -x

module="stage_interval_xml_files"
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
script_name="$(basename ${BASH_SOURCE[0]})"
source "${script_dir}/.hes_intvl_nonvee_env"
source "${COMMON_PATH}/scripts/shell_utils.sh" || { echo "ERROR: failed to source ${COMMON_PATH}/scripts/shell_utils.sh"; exit 1; } 

MODULE="stage_interval_xml_files"
PRIOR_MODULE="source_interval_data_files"

# Log file setup
LOG_DIR="${INTVL_NONVEE_LOGS}"
mkdir -p ${LOG_DIR}
LOG_FILE="${LOG_DIR}/${module}_$(date +'%Y%m%d_%H%M%S').log"

# Logging function
fnLog() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') $1" | tee -a "${LOG_FILE}"
}

log_error_exit() {
    fnLog "ERROR: $1"
    exit 1
}

# Command-line argument handling
while getopts ":o:p:" opt; do
    case $opt in
        o) VAR_OPCO=$OPTARG ;;
        p) pdate=$OPTARG ;;
        \?) log_error_exit "Invalid option -$OPTARG" ;;
        :) log_error_exit "Option -$OPTARG requires an argument." ;;
    esac
done

if [ -z "${VAR_OPCO}" ] || [ -z "${pdate}" ]; then
    log_error_exit "Both -o (VAR_OPCO) and -p (pdate) options are required."
fi

fnLog "Script started with OPCO: ${VAR_OPCO} and pdate: ${pdate}"

check_completion() {
    if [ ! -f "${INTVL_NONVEE_DATA}/${MODULE}_${VAR_OPCO}.complete" ]; then
        fnLog "ERROR: In-COMPLETE EXIT !!!!!!!!!!!"
    else
        fnLog "Script Completed start to end - ${INTVL_NONVEE_DATA}/${MODULE}_${VAR_OPCO}.complete !!!!!!!!!!!"
    fi
}

fnLog "========================== ${MODULE} CREATE RUNNING FILE  =========================="

prior_job_check=$(ls -1 "${INTVL_NONVEE_DATA}/${PRIOR_MODULE}_${VAR_OPCO}.complete" 2>/dev/null)
ret=$?
if [ $ret -ne 0 ]; then
    fnLog "Missing Prior Job Completion Checkpoint File : ${INTVL_NONVEE_DATA}/${PRIOR_MODULE}_${VAR_OPCO}.complete"
    log_error_exit
fi

rm -f "${INTVL_NONVEE_DATA}/${MODULE}_${VAR_OPCO}.complete"
echo "${pdate}" > "${INTVL_NONVEE_DATA}/${MODULE}_${VAR_OPCO}.running"

fnLog "========================== ${MODULE} START =========================="
SPARK_SETTING="${INTVL_NONVEE_PARMS}/interval_xml_spark_setting.parm"
SPARK_SCRIPT="${INTVL_NONVEE_DATA}/${MODULE}_${VAR_OPCO}_${pdate}.sql"
spark_stage_tbl="${INTVL_NONVEE_BIN}/ddl/stg_nonvee.interval_data_files_${VAR_OPCO}_stg.ddl"
spark_stage_vw="${INTVL_NONVEE_BIN}/ddl/stg_nonvee.interval_data_files_${VAR_OPCO}_stg_vw.ddl"
fnLog "Processing for OPCO : ${VAR_OPCO}"

## Drop & Recreate the Staging Table First
fnLog "spark-sql --conf-file ${spark_stage_tbl} --master yarn --deploy-mode client"
spark-sql --conf-file "${spark_stage_tbl}" --master yarn --deploy-mode client 2>&1
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    fnLog "ERROR: Failure in spark-sql --conf-file ${spark_stage_tbl} !!!"  
    log_error_exit
fi

## Drop & Recreate the Staging View First
fnLog "spark-sql --conf-file ${spark_stage_vw} --master yarn --deploy-mode client"
spark-sql --conf-file "${spark_stage_vw}" --master yarn --deploy-mode client 2>&1
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    fnLog "ERROR: Failure in spark-sql --conf-file ${spark_stage_vw} !!!"  
    log_error_exit
fi

cat <<EOF >> ${SPARK_SCRIPT}
SET spark.sql.sources.partitionOverwriteMode=dynamic;
INSERT OVERWRITE TABLE stg_nonvee.interval_data_files_${VAR_OPCO}_stg
SELECT
    filename,
    '${VAR_OPCO}' as aep_opco,
    a.metername as serialnumber,
    a.utildeviceid,
    a.macid,
    a.intervallength,
    a.blockstarttime,
    a.blockendtime,
    a.numberintervals,
    a.starttime,
    a.endtime,
    a.interval_epoch,
    a.int_gatewaycollectedtime,
    a.int_blocksequencenumber,
    a.int_intervalsequencenumber,
    a.channel,
    a.value as aep_raw_value,
    a.value,
    a.uom as aep_raw_uom,
    upper(a.aep_uom) as aep_derived_uom,
    a.name_register,
    a.aep_sqi as aep_srvc_qlty_idntfr,
    nvl(macs.prem_nb, null) as aep_premise_nb,
    nvl(macs.bill_cnst, null) as bill_cnst,
    nvl(macs.acct_clas_cd, null) as aep_acct_cls_cd,
    nvl(macs.acct_type_cd, null) as aep_acct_type_cd,
    nvl(macs.devc_cd, null) as aep_devicecode,
    nvl(macs.pgm_id_nm, null) as aep_meter_program,
    nvl(macs.sd, null) as aep_srvc_dlvry_id,
    nvl(macs.mtr_pnt_nb, null) as aep_mtr_pnt_nb,
    nvl(macs.tarf_pnt_nb, null) as aep_tarf_pnt_nb,
    nvl(macs.cmsg_mtr_mult_nb,null) as aep_comp_mtr_mltplr,
    macs.mtr_rmvl_ts as aep_mtr_removal_ts,
    macs.mtr_inst_ts as aep_mtr_install_ts,
    macs.aep_city,
    substr(macs.aep_zip,1,5) as aep_zip,
    macs.aep_state,
    '${HDP_UPDATE_USER}' as hdp_update_user,
    a.part_date,
    a.value_mltplr_flg,
    substr(trim(a.metername),-2) as aep_meter_bucket
FROM stg_nonvee.interval_data_files_${VAR_OPCO}_src_vw a
LEFT JOIN (
    SELECT
        prem_nb,
        bill_cnst,
        acct_clas_cd,
        acct_type_cd,
        devc_cd,
        pgm_id_nm,
        concat(nvl(tx_mand_data,''),nvl(doe_nb,''),nvl(serv_deliv_id,'')) sd,
        mfr_devc_ser_nbr,
        mtr_inst_ts,
        mtr_rmvl_ts,
        acct_turn_on_dt,
        acct_turn_off_dt,
        bill_acct_nb,
        mtr_pnt_nb,
        tarf_pnt_nb,
        null as cmsg_mtr_mult_nb,
        unix_timestamp(mtr_inst_ts, "yyyy-MM-dd HH:mm:ss") as unix_mtr_inst_ts,
        CASE mtr_rmvl_ts
            WHEN '9999-01-01' THEN unix_timestamp(mtr_rmvl_ts, "yyyy-MM-dd")
            ELSE unix_timestamp(mtr_rmvl_ts, "yyyy-MM-dd HH:mm:ss")
        END as unix_mtr_rmvl_ts,
        aep_city,
        aep_state,
        aep_zip
    FROM mdm.meter_asset
) macs
ON a.metername = macs.mfr_devc_ser_nbr;
EOF

fnLog "========================== ${MODULE} END =========================="
check_completion
