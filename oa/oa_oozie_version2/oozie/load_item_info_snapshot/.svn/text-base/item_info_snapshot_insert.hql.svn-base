-----------------------------------------------------------------------------------------
--  Process Name    : OA Automation - Pre-process
--  Script Name     : item_info_snapshot_insert.hql
--  Description     : This hql will extracts & loads item, dept & subcat from itemInfoSourceTbl table. 
--  Modification Log: 
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  vmalaya            1.0              09/25/2014          Centralization of OA 
--  phala1             2.0              12/11/2014          OA support multiple runs                                                                                   
-----------------------------------------------------------------------------------------
SET hive.exec.compress.output=false;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;

INSERT OVERWRITE TABLE ${itemInfoTbl} PARTITION(campaign_iter='${campaign_iter}')
SELECT DISTINCT
         mds_fam_id AS system_item_nbr
        ,dept_nbr AS category_nbr
        ,subclass_nbr AS sub_category_nbr
FROM ${itemInfoSourceTbl}
WHERE snapshot_beg_date <= '${snapshot_dt}' AND snapshot_end_date >= '${snapshot_dt}' AND base_div_nbr = ${base_div_nbr}
;
