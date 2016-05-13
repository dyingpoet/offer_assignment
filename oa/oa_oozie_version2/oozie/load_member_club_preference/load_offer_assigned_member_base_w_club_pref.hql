-----------------------------------------------------------------------------------------
--  Process Name    : OA Automation - Pre-process
--  Script Name     : load_offer_assigned_member_base_w_club_pref.hql
--  Description     : This hql will extracts & loads membership details into member_club_pref_table table. 
--  Modification Log: 
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

INSERT OVERWRITE TABLE ${member_club_pref_table} PARTITION(campaign_iter='${campaign_iter}')
SELECT DISTINCT
	a.membership_nbr AS membership_nbr
	, c.cardholder_nbr AS cardholder_nbr
	, b.membership_create_date AS membership_create_date
	, IF(c.preferred_club_nbr IS NULL AND b.assigned_club_nbr IS NULL, b.issuing_club_nbr, c.preferred_club_nbr) AS preferred_club_nbr
	, b.issuing_club_nbr AS issuing_club_nbr
	, b.assigned_club_nbr AS assigned_club_nbr
FROM 
	${offer_assigned_member_base_table} a
JOIN 
	${sams_membership_dim_table} b
ON (a.membership_nbr = b.membership_nbr AND a.membership_create_date = b.membership_create_date AND b.current_ind = 'Y' AND b.membership_obsolete_date>='${ds_obsolete_date_lb}' AND a.member_list_id=${member_list_nbr})
JOIN 
	${sams_cardholder_dim_table} c
ON (a.membership_nbr = c.membership_nbr AND a.cardholder_nbr=c.cardholder_nbr AND a.membership_create_date = c.membership_create_date AND c.current_ind='Y')
;
