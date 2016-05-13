-----------------------------------------------------------------------------------------
--  Process Name    : OA Automation - Pre-process
--  Script Name     : trans_insert.hql
--  Description     : This hql will extracts & loads data to partition table. 
--  Modification Log: Modified partition to support multiple runs
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  vmalaya            1.0              09/25/2014          Centralization of OA 
--  phala1             2.0              12/11/2014          OA support multiple runs                                                                                   
-----------------------------------------------------------------------------------------
SET hive.exec.compress.output=false;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;

INSERT OVERWRITE TABLE ${transTbl} PARTITION(campaign_iter='${campaign_iter}')
SELECT visit_nbr,
  store_nbr,
  unit_qty,
  retail_price,
  card_holder_nbr,
  membership_nbr,
  scan_id AS system_item_nbr,
  category_nbr,
  sub_category_nbr,
  visit_date
FROM
(SELECT scan_data.visit_nbr AS visit_nbr,
        scan_data.store_nbr AS store_nbr,
        scan_id,
        unit_qty,
        retail_price,
        card_holder_nbr,
        membership_nbr,
        scan_data.visit_date
FROM
(SELECT visit_nbr,
        store_nbr,
        scan_id,
        unit_qty,
        retail_price,
        visit_date
      --FROM us_wc_mb_tables.scan
    FROM ${scanTbl}
    WHERE visit_date >= '${StartDate}' AND visit_date <= '${EndDate}') scan_data 
	JOIN
       (SELECT store_nbr,
               visit_nbr,
               card_holder_nbr,
               membership_nbr,
               visit_date
               --FROM us_wc_mb_tables.visit_member
        FROM ${visitMemberTbl}
        WHERE visit_date >= '${StartDate}' AND visit_date <= '${EndDate}'
        AND mbr_country_code = '${country_code}') visit_mem_info
        ON (visit_mem_info.store_nbr = scan_data.store_nbr AND visit_mem_info.visit_nbr = scan_data.visit_nbr)) scan_visit_data 
    JOIN
        (SELECT mds_fam_id,
                dept_nbr AS category_nbr,
                subclass_nbr AS sub_category_nbr
                --FROM common_transaction_model.item_dim
         FROM ${itemDimTbl}
         WHERE country_code = '${country_code}' AND
               base_div_nbr = ${base_div_nbr} AND
               current_ind = '${current_ind}')item_info
         ON (scan_visit_data.scan_id = item_info.mds_fam_id)
;
