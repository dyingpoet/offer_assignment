-----------------------------------------------------------------------------------------
--  Process Name    : OA Automation - Pre-process
--  Script Name     : load_pis_member_subcat_coupon.hql
--  Description     : This hql will extracts & loads member subcat detail into task table.
--  Input Tables    : member_club_pref_table, subcat_item_club_table,reco_table
--  Output Table    : task_table
--  Modification Log: Modified partition to support multiple runs
-----------------------------------------------------------------------------------------
--  Author           Version             Date                  Comments
-----------------------------------------------------------------------------------------
--  vmalaya           1.0              09/18/2014          Baseline
--  sgopa4            2.0              11/18/2014          OA Parameter Changes
--  phala1            3.0              12/11/2014          OA support multiple runs
-----------------------------------------------------------------------------------------

SET hive.exec.compress.output=false;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;

USE ${database};

--ALTER TABLE ${reco_table} ADD IF NOT EXISTS PARTITION(campaign_iter='${campaign_iter}') LOCATION '${recoLoc}';

INSERT OVERWRITE TABLE ${task_table} PARTITION (campaign_iter = '${campaign_iter}')
SELECT
a.membership_nbr,a.cardholder_nbr,a.membership_create_date,a.cat_subcat_nbr,a.system_item_nbr,a.value_coupon_nbr
, b.score
, '0000' AS anchor_cat_subcat_nbr
FROM
(
    SELECT DISTINCT
       a.membership_nbr AS membership_nbr
       , a.cardholder_nbr AS cardholder_nbr
       , a.membership_create_date AS membership_create_date
       , a.cat_subcat_nbr AS cat_subcat_nbr
       , a.system_item_nbr AS system_item_nbr
       , a.value_coupon_nbr AS value_coupon_nbr
    FROM
    (
        SELECT DISTINCT
            a.membership_nbr AS membership_nbr
            , a.cardholder_nbr AS cardholder_nbr
            , a.membership_create_date AS membership_create_date
            , b.cat_subcat_nbr AS cat_subcat_nbr
            , b.system_item_nbr AS system_item_nbr
            , b.value_coupon_nbr AS value_coupon_nbr
        FROM
        ${member_club_pref_table} a
        JOIN
        ${subcat_item_club_table} b
        ON (a.preferred_club_nbr = b.club_nbr AND a.campaign_iter='${campaign_iter}' AND b.campaign_iter='${campaign_iter}')

        UNION ALL

        SELECT DISTINCT
            a.membership_nbr AS membership_nbr
            , a.cardholder_nbr AS cardholder_nbr
            , a.membership_create_date AS membership_create_date
            , b.cat_subcat_nbr AS cat_subcat_nbr
            , b.system_item_nbr AS system_item_nbr
            , b.value_coupon_nbr AS value_coupon_nbr
        FROM
        ${member_club_pref_table} a
        JOIN
        ${subcat_item_club_table} b
        ON (a.assigned_club_nbr = b.club_nbr AND a.campaign_iter='${campaign_iter}' AND b.campaign_iter='${campaign_iter}')
    ) a
) a
JOIN ${reco_table} b
ON (a.membership_nbr = b.membership_nbr AND a.cardholder_nbr = b.cardholder_nbr AND a.membership_create_date = b.membership_create_date
AND a.cat_subcat_nbr = b.catsubcat_nbr AND b.score_type='${campaign_score_type}');
