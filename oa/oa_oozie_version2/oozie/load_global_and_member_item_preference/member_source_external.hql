-----------------------------------------------------------------------------------------
--  Process Name : OA Automation - Pre-process
--  Script Name  : member_source_external.hql
--  Description  : Extract member pool file for selected 'member_list_nbr' & 'member_list_ts' 
--				   from UNICA table for OA pre-processing.
--  Modification Log:  
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  dnaga1             1.0              01/13/2015          OA SPE changes
-----------------------------------------------------------------------------------------

INSERT OVERWRITE TABLE member_pool PARTITION(campaign_iter='${campaign_iter}')
SELECT distinct
 membership_nbr
,membership_create_date
,country_code as issuing_country_code
,cardholder_nbr
, ' ' as pdcardholder_nbr
FROM ${unica_predefined_member_list}
WHERE member_list_id='${member_list_nbr}'
AND member_list_ts='${member_list_ts}'
;