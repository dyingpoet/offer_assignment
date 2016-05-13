-----------------------------------------------------------------------------------------
--  Process Name    : OA Automation - Pre-process
--  Script Name     : step1_skip_item.hql
--  Description     : This hql will load fia_subcat_item_club table with checking club inventory for item.
--  Modification Log: Modified for version3
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  sgopa4            1.0              02/17/2015
-----------------------------------------------------------------------------------------


SET hive.exec.compress.output=false;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true
SET mapred.job.queue.name=mep;

USE ${database};

-- skip inventory check
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
ON (a.mds_fam_id = b.item_nbr)
--LEFT OUTER JOIN ${dept_list} d
--ON a.dept_nbr = d.dept_nbr
WHERE a.campaign_nbr = ${campaign_id}
AND b.status IN ('A', 'S', 'O')
AND a.dept_nbr IN (79,76,72,77,93,84,56,48,85,97,91,27,88)
AND a.mds_fam_id NOT IN (2252057,4670329,31171310,31413539,31409873,985746,5756086,2964987,2406371) 
;


-- NOT skip inventory check
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
--LEFT OUTER JOIN ${dept_list} d
--ON a.dept_nbr = d.dept_nbr
WHERE a.campaign_nbr = ${campaign_id}
AND b.onsite_onhand_qty >0
AND b.status IN ('A', 'S', 'O')
AND a.dept_nbr NOT IN (79,76,72,77,93,84,56,48,85,97,91,27,88)
AND a.mds_fam_id NOT IN (2252057,4670329,31171310,31413539,31409873,985746,5756086,2964987,2406371)
;


-- skip any inventory / status check




INSERT INTO TABLE ${fia_subcat_item_club} PARTITION(campaign_iter='${campaign_iter}')
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
ON (a.mds_fam_id = b.item_nbr)
--LEFT OUTER JOIN ${dept_list} d
--ON a.dept_nbr = d.dept_nbr
WHERE a.campaign_nbr = ${campaign_id}
AND a.mds_fam_id IN (2252057,4670329,31171310,31413539,31409873,985746,5756086,2964987,2406371)
;
