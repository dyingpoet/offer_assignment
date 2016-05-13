set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;

USE ${hiveconf:Database};

--CREATE EXTERNAL TABLE IF NOT EXISTS pythia.offer_assignment_output_delivery (
CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:task_table} (
    campaign_id INT
    , vt_individual_customer_id INT
    , vt_customer_household_id INT
    , experian_household_id INT
    , experian_individual_id INT
    , issuing_country_code STRING
    , membership_nbr STRING
    , masked_membership_nbr STRING
    , cardholder_nbr STRING
    , membership_create_date STRING
    , val_coupon_nbr STRING
    , treatment_type_cd STRING
    , offer_type_desc STRING
    , assignment_type_desc STRING
    , package_code STRING
    , item_nbr INT
    , is_item_nbr INT
    , start_date STRING
    , end_date STRING
    , p_score_source STRING
    , p_score_nbr STRING
    , p_score_type_cd STRING
    , offer_ind STRING
    , nonoffer_item_nbr_mapped INT
)
PARTITIONED BY (segment STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
LOCATION '${hiveconf:task_path}'
--LOCATION '/user/pythia/Workspaces/SamsMEP/offer_assignment_output_delivery'
;

