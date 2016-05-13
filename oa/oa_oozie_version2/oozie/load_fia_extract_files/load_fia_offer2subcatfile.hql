-----------------------------------------------------------------------------------------
--  Process Name : OA Automation - Pre-process
--  Script Name  : load_fia_offer2subcatfile.hql
--  Description  : This hql will loads distinct value coupon nbr and subcat nbr to table
--  Modification Log: Partition added to support multiple run 
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  vmalaya            1.0              09/25/2014          Centralization of OA 
--  phala1             2.0              12/17/2014          Multiple Run Support                                                                             -----------------------------------------------------------------------------------------

insert overwrite table ${offer2subcatfile} PARTITION(campaign_iter='${campaign_iter}')
select distinct value_coupon_nbr, concat(lpad(dept_nbr, 2, '0'), lpad(subclass_nbr, 2, '0')) as cat_subcat_nbr
from ${FiaTbl}
where campaign_nbr = ${fia_campaign_no}
--and run = ${fia_version}
order by value_coupon_nbr;
