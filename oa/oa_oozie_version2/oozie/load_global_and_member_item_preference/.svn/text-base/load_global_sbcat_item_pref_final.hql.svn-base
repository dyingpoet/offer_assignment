-----------------------------------------------------------------------------------------
--  Process Name    : OA Automation - Pre-process
--  Script Name     : load_global_sbcat_item_pref_final.hql
--  Description     : This hql will extracts & loads rank detail for item & subcat from global-sbcat_item_pref table. 
--  Modification Log: Modified partition to support multiple runs
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  vmalaya            1.0              09/25/2014          Centralization of OA 
--  phala1             2.0              12/11/2014          OA support multiple runs                                                                                   
-----------------------------------------------------------------------------------------
USE ${database};

ALTER TABLE global_sbcat_item_pref ADD IF NOT EXISTS PARTITION(campaign_iter='${campaign_iter}') 
LOCATION '/user/hive/warehouse/${database}.db/global_sbcat_item_pref/campaign_iter=${campaign_iter}';

ADD jar ${jarpath}/CARUtils.jar;
CREATE TEMPORARY FUNCTION p_rank AS 'com.walmart.ckp.car.udf.RankUDF';

INSERT OVERWRITE TABLE global_subcat_item_pref_final PARTITION(campaign_iter='${campaign_iter}')
SELECT 
    a.cat_subcat_nbr AS cat_subcat_nbr, 
    a.system_item_nbr AS system_item_nbr,
    p_rank(a.cat_subcat_nbr) AS Rank
FROM (SELECT 
    cat_subcat_nbr, 
    system_item_nbr,
    countVisits
FROM global_sbcat_item_pref
WHERE campaign_iter='${campaign_iter}'
DISTRIBUTE BY  cat_subcat_nbr
SORT BY cat_subcat_nbr, system_item_nbr, countVisits desc) a
;  
