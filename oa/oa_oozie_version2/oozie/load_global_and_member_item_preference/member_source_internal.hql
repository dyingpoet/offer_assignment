-----------------------------------------------------------------------------------------
--  Process Name : OA Automation - Pre-process
--  Script Name  : member_source_internal.hql
--  Description  : Extract member pool file for selected 'member_list_nbr' for OA pre-processing.
--  Modification Log:  
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  dnaga1             1.0              01/13/2015          OA SPE changes
-----------------------------------------------------------------------------------------
SET hive.exec.compress.output=false;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;

USE ${database};

INSERT OVERWRITE TABLE member_pool PARTITION (campaign_iter='${campaign_iter}')
SELECT
 membership_nbr
,membership_create_date
,country_code as issuing_country_code
,cardholder_nbr
,paidcardholder_id
FROM 
${spe_member_list}
WHERE member_list_id='${member_list_nbr}'
;
