-----------------------------------------------------------------------------------------
--  Process Name    : OA Automation - Pre-process
--  Script Name     : load_fia_subcat_item_club.hql
--  Description     : This hql will extract detail from Inventory history and OA fia tables.
--  Modification Log: Modified partition to support multiple runs
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  vmalaya            1.0              09/25/2014          Centralization of OA
--  phala1             2.0              12/11/2014          OA support multiple runs
-----------------------------------------------------------------------------------------
SET hive.exec.compress.output=false;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;

USE ${database};


INSERT OVERWRITE TABLE ${fia_subcat_item_club} PARTITION(campaign_iter='${campaign_iter}')
SELECT
        concat(lpad(a.dept_nbr, 2, '0'), lpad(a.subclass_nbr, 2, '0')) AS cat_subcat_nbr
         ,a.mds_fam_id AS system_item_nbr
         ,a.value_coupon_nbr AS value_coupon_nbr
         ,b.club_nbr AS club_nbr
FROM ${offer_assignment_fia_table} a
JOIN (select club_nbr, status, item_nbr 
FROM ${club_item_inventory_history_table}
WHERE rundate = '${inventory_ds}') b
ON (a.mds_fam_id = b.item_nbr )
WHERE a.campaign_nbr = ${campaign_id}
--and a.run = ${fia_version}
AND (b.status='A' or b.status='S')
;