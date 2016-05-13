SET hive.exec.compress.output=false;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;

USE ${hiveconf:Database};

--CREATE EXTERNAL TABLE IF NOT EXISTS pythia.pis_member_subcat_item_coupon_score (
CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:task_table} (
                membership_nbr INT
                , cardholder_nbr SMALLINT
                , membership_create_date STRING
                , cat_subcat_nbr STRING
                , system_item_nbr INT
                , value_coupon_nbr INT
                , score FLOAT
                , anchor_cat_subcat_nbr STRING
)
PARTITIONED BY (campaign_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
LOCATION '${hiveconf:task_path}'
--LOCATION '/user/pythia/Workspaces/SamsMEP/pis_member_subcat_item_coupon_score'
;


--CREATE EXTERNAL TABLE IF NOT EXISTS pythia.fia_subcat_item_club (   
CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:subcat_item_club_table} (   
                cat_subcat_nbr STRING
                , system_item_nbr INT
                , club_nbr INT
)
PARTITIONED BY (campaign_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE 
LOCATION '${hiveconf:subcat_item_club_path}'
--LOCATION '/user/pythia/Workspaces/SamsMEP/fia_subcat_item_club'
;


--CREATE EXTERNAL TABLE IF NOT EXISTS pythia.offer_assigned_member_base_w_club_preference (   
CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:member_club_pref_table} (   
                membership_nbr INT
                , cardholder_nbr SMALLINT
                , membership_create_date STRING
                , preferred_club_nbr INT
                , issuing_club_nbr INT
                , assigned_club_nbr INT
)
PARTITIONED BY (campaign_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE 
LOCATION '${hiveconf:member_club_pref_path}'
--LOCATION '/user/pythia/Workspaces/SamsMEP/offer_assigned_member_base_w_club_preference'
;


-- --CREATE EXTERNAL TABLE IF NOT EXISTS pythia.pis_member_item_subcat_score (
-- CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:member_item_subcat_score_table} (
--                 membership_nbr INT
--                 , cardholder_nbr SMALLINT
--                 , membership_create_date STRING
--                 , system_item_nbr INT
--                 , cat_subcat_nbr STRING
--                 , score FLOAT
-- )
-- PARTITIONED BY (campaign_month STRING)
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
-- LOCATION '${hiveconf:member_item_subcat_score_path}'
-- --LOCATION '/user/pythia/Workspaces/SamsMEP/pis_member_item_subcat_score'
-- ;

