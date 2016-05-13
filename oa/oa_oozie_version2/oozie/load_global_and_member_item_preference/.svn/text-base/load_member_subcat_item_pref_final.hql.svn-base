-----------------------------------------------------------------------------------------
--  Process Name    : OA Automation - Pre-process
--  Script Name     : load_member_subcat_item_pref_final.hql
--  Description     : This hql will extracts & loads rank detail for member, cardholder, create-date,
--					  item & subcat from member_subcat_item_pref table. 
--  Modification Log: Modified partition to support multiple runs
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  vmalaya            1.0              09/25/2014          Centralization of OA 
--  phala1             2.0              12/11/2014          OA support multiple runs                                                                                   
-----------------------------------------------------------------------------------------
USE ${database};

ALTER TABLE member_subcat_item_pref ADD IF NOT EXISTS PARTITION(campaign_iter='${campaign_iter}') 
LOCATION '/user/hive/warehouse/${database}.db/member_subcat_item_pref/campaign_iter=${campaign_iter}';

add jar ${jarpath}/CARUtils.jar;
CREATE TEMPORARY FUNCTION p_rank AS 'com.walmart.ckp.car.udf.RankUDF';

INSERT OVERWRITE TABLE member_subcat_item_pref_final PARTITION(campaign_iter='${campaign_iter}')
SELECT 
    membership_nbr, (a.cardholder_nbr) AS pdcardholder_nbr, membership_create_date, cat_subcat_nbr, system_item_nbr,
    p_rank(a.cat_subcat_nbr) AS Rank
FROM (SELECT 
    membership_nbr, cardholder_nbr, membership_create_date, cat_subcat_nbr, system_item_nbr, visit_count
FROM member_subcat_item_pref
WHERE visit_count > 0 AND campaign_iter='${campaign_iter}'
    DISTRIBUTE BY  membership_nbr, cardholder_nbr, membership_create_date, cat_subcat_nbr
    SORT BY membership_nbr, cardholder_nbr, membership_create_date, cat_subcat_nbr, system_item_nbr, visit_count desc) a
;
