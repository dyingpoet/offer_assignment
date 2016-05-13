-----------------------------------------------------------------------------------------
--  Process Name : OA Automation - Pre-process
--  Script Name  : load_fia_sc_coupon_cnt.hql
--  Description  : This hql will loads distinct subcat_nbr, value_coupon_nbr and investment_cnt
--					to table based on campaign_id
--  Modification Log: Added partition to support multiple runs
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  vmalaya            1.0              09/25/2014          Centralization of OA 
--  phala1             2.0              12/17/2014          Multiple Run Support                           
-----------------------------------------------------------------------------------------

insert overwrite table ${sc_coupon_cnt_tbl} PARTITION(campaign_iter='${campaign_iter}')
select distinct concat(lpad(dept_nbr, 2, '0'), lpad(subclass_nbr, 2, '0')) as cat_subcat_nbr, value_coupon_nbr, investment_cnt
from ${FiaTbl}
where campaign_nbr = ${fia_campaign_no}
--and run = ${fia_version}
order by value_coupon_nbr;
