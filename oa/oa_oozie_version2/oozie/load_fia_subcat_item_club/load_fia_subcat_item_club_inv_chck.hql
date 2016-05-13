-----------------------------------------------------------------------------------------
--  Process Name    : OA Automation - Pre-process
--  Script Name     : load_fia_subcat_item_club_inv_chck.hql
--  Description     : This hql will extract detail from Inventory history and OA fia tables
--                    if the inventory indicator is 'Y'.
--  Modification Log:
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  vmalaya            1.0              09/25/2014          Centralization of OA
--  nkuland            2.0              11/07/2014          OA Ver 2.0
--  phala1             3.0              12/11/2014          OA support multiple runs
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
FROM
    (SELECT club_nbr,status,item_nbr 
     FROM ${club_item_inventory_history_table} 
	 WHERE rundate='${inventory_ds}')b
JOIN
     ${offer_assignment_fia_table} a
ON
  ( a.mds_fam_id = b.item_nbr  )
JOIN ${dept_list} c
ON a.dept_nbr = c.dept_nbr
WHERE a.campaign_nbr = ${campaign_id}
AND   b.status IN ('A', 'S', 'C', 'O')
-- AND   a.dept_nbr IN (79,76,72,77,93,84,56,48,85,97,91,27,88)
;

INSERT INTO TABLE ${fia_subcat_item_club} PARTITION(campaign_iter='${campaign_iter}')
SELECT
        concat(lpad(a.dept_nbr, 2, '0'), lpad(a.subclass_nbr, 2, '0')) AS cat_subcat_nbr
         ,a.mds_fam_id AS system_item_nbr
         ,a.value_coupon_nbr AS value_coupon_nbr
         ,b.club_nbr AS club_nbr
FROM
    (SELECT club_nbr,status,item_nbr,onsite_onhand_qty 
	 FROM ${club_item_inventory_history_table} 
	 WHERE rundate='${inventory_ds}')b
JOIN
    ${offer_assignment_fia_table} a
ON (a.mds_fam_id = b.item_nbr)
LEFT OUTER JOIN ${dept_list} d
ON a.dept_nbr = d.dept_nbr
WHERE d.dept_nbr IS NULL
AND a.campaign_nbr = ${campaign_id}
-- AND b.onsite_onhand_qty >0 
AND b.status IN ('A', 'S', 'C', 'O')
-- AND a.dept_nbr NOT IN (79,76,72,77,93,84,56,48,85,97,91,27,88)
;
